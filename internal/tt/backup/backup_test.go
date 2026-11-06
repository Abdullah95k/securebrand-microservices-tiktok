package backup

import (
	"context"
	"errors"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/minio/minio-go/v7"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/s3x"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

func TestKey(t *testing.T) {
	at := time.Date(2026, 11, 7, 23, 17, 5, 0, time.FixedZone("x", 3*3600))
	if got, want := Key(Options{Prefix: "tks/"}, at), "tks/postgres/2026/11/07/tks-20261107T201705Z.dump"; got != want {
		t.Fatalf("Key = %q, want %q", got, want)
	}
	if !dumpName.MatchString(Key(Options{}, at)) {
		t.Fatal("List would not see a dump Run wrote")
	}
}

// The password goes to PGPASSWORD, never the command line (visible to
// every process in the pod).
func TestConnHidesPassword(t *testing.T) {
	target, pw, err := conn("postgres://tks:s3cr%40t@db.internal:25060/tks?sslmode=verify-full&sslrootcert=%2Ftmp%2Fca.pem")
	if err != nil {
		t.Fatal(err)
	}
	if pw != "s3cr@t" || strings.Contains(target, "s3cr") || !strings.Contains(target, "tks@db.internal:25060/tks") || !strings.Contains(target, "sslrootcert") {
		t.Fatalf("target %q password %q", target, pw)
	}
	if _, _, err := conn("host=db user=tks"); err == nil {
		t.Fatal("a keyword DSN must be refused")
	}
}

// tools returns pg_dump and pg_restore for the test server
// (TKS_TEST_PG_DUMP and TKS_TEST_PG_RESTORE, default from PATH), skipping
// when they are older than the server: pg_dump refuses a newer server.
func tools(t *testing.T, db *pgxpool.Pool) Options {
	t.Helper()
	o := Options{Bucket: "tks-backups", Prefix: "tks/", PartMB: 5,
		PgDump: os.Getenv("TKS_TEST_PG_DUMP"), PgRestore: os.Getenv("TKS_TEST_PG_RESTORE")}
	o.PgDump, o.PgRestore = bin(o.PgDump, "pg_dump"), bin(o.PgRestore, "pg_restore")
	var server int
	if err := db.QueryRow(context.Background(), `SELECT current_setting('server_version_num')::int / 10000`).Scan(&server); err != nil {
		t.Fatal(err)
	}
	for _, tool := range []string{o.PgDump, o.PgRestore} {
		major, err := ToolMajor(context.Background(), tool)
		if err != nil || major < server {
			if os.Getenv("CI") != "" {
				t.Fatalf("%s: version %d (%v), server %d: CI must test the backup", tool, major, err, server)
			}
			t.Skipf("%s: version %d (%v), server %d", tool, major, err, server)
		}
	}
	return o
}

func store(t *testing.T) *minio.Client {
	t.Helper()
	srv := httptest.NewServer(gofakes3.New(s3mem.New()).Server())
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	c, err := s3x.Open(context.Background(), u.Host, "k", "secretsecret", "us-east-1", false, "tks-backups")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// fingerprint is every tt table's rows (partitions each on their own), as
// a hash per table, and the migrations' version.
func fingerprint(t *testing.T, db *pgxpool.Pool) map[string]string {
	t.Helper()
	ctx := context.Background()
	rows, err := db.Query(ctx, `SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'tt' AND c.relkind = 'r' ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, name)
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	out := map[string]string{}
	for _, name := range tables {
		var n int64
		var sum *string
		q := `SELECT count(*), md5(string_agg(r::text, ',' ORDER BY r::text)) FROM tt.` + name + ` r`
		if err := db.QueryRow(ctx, q).Scan(&n, &sum); err != nil {
			t.Fatal(name, err)
		}
		s := ""
		if sum != nil {
			s = *sum
		}
		out[name] = s
	}
	return out
}

// TestBackupRestore: a dump taken by the read-only role (as the backup job
// runs) restores into a new database with every table's rows equal, the
// roles can be granted on it, and a damaged or failed dump is caught.
func TestBackupRestore(t *testing.T) {
	ctx := context.Background()
	dsn := testenv.EmptyPostgres(t)
	owner, err := ttstore.Open(ctx, dsn, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(owner.Close)
	o := tools(t, owner)
	if err := ttstore.MigrateUp(ctx, owner); err != nil {
		t.Fatal(err)
	}
	// Synthetic rows, in partitioned and plain tables.
	for _, q := range []string{
		`INSERT INTO tt.accounts (user_id, sec_uid, username, nickname, bio, author_key) VALUES
			(1001, 'MS4wLjABAAAAsynthetic1', 'example_one', 'Example ✓', 'line one
line two', '\x0102'), (1002, 'MS4wLjABAAAAsynthetic2', 'example_two', NULL, '', '\x0304')`,
		`SELECT tt.ensure_partition('posts', 'posts_y2026m11', '2026-11-01', '2026-12-01', NULL)`,
	} {
		if _, err := owner.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	reader, readerDSN := testenv.Role(t, owner, dsn)
	if _, err := ttstore.Grant(ctx, owner, config.PostgresRoles{Reader: reader}); err != nil {
		t.Fatal(err)
	}
	before := fingerprint(t, owner)
	if before["accounts"] == "" {
		t.Fatal("no rows to back up")
	}

	s3 := store(t)
	at := time.Date(2026, 11, 7, 20, 17, 0, 0, time.UTC)
	d, err := Run(ctx, s3, readerDSN, o, at)
	if err != nil {
		t.Fatal(err)
	}
	if d.Key != "tks/postgres/2026/11/07/tks-20261107T201700Z.dump" || d.Bytes == 0 || len(d.SHA256) != 64 {
		t.Fatalf("dump %+v", d)
	}
	// A second, later dump: latest picks it.
	if _, err := Run(ctx, s3, readerDSN, o, at.Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	all, err := List(ctx, s3, o)
	if err != nil || len(all) != 2 || all[0].Key != d.Key {
		t.Fatalf("List = %+v, %v", all, err)
	}

	target := testenv.EmptyPostgres(t)
	tdb, err := ttstore.Open(ctx, target, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tdb.Close)
	got, err := Restore(ctx, s3, target, o, d.Key, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.SHA256 != d.SHA256 || got.Bytes != d.Bytes {
		t.Fatalf("restored %+v, dumped %+v", got, d)
	}
	after := fingerprint(t, tdb)
	if len(after) != len(before) {
		t.Fatalf("tables: %d restored, %d dumped", len(after), len(before))
	}
	for name, sum := range before {
		if after[name] != sum {
			t.Errorf("table %s differs after the restore", name)
		}
	}
	// The restored schema is current, and the roles can be granted on it
	// (as `tksctl migrate up` does after a restore).
	if err := ttstore.MigrateUp(ctx, tdb); err != nil {
		t.Fatal(err)
	}
	if err := ttstore.CheckSchema(ctx, tdb); err != nil {
		t.Fatal(err)
	}
	r2, _ := testenv.Role(t, tdb, target)
	if _, err := ttstore.Grant(ctx, tdb, config.PostgresRoles{Reader: r2, Crawler: r2 + "x"}); err == nil {
		t.Fatal("a missing role must fail the grant")
	}
	if _, err := ttstore.Grant(ctx, tdb, config.PostgresRoles{Reader: r2}); err != nil {
		t.Fatal(err)
	}

	// Into a database that has schema tt: refused.
	has := func(context.Context) (bool, error) { return true, nil }
	if _, err := Restore(ctx, s3, target, o, "latest", has); !errors.Is(err, ErrNotEmpty) {
		t.Fatalf("restore over a schema: %v", err)
	}

	// A damaged dump: its checksum no longer matches.
	obj, err := s3.GetObject(ctx, o.Bucket, d.Key, minio.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	buf := make([]byte, 1<<16)
	for {
		n, rerr := obj.Read(buf)
		b.Write(buf[:n])
		if rerr != nil {
			break
		}
	}
	_ = obj.Close()
	damaged := []byte(b.String())
	damaged = append(damaged, 0) // a byte past the end pg_restore never reads
	if _, err := s3.PutObject(ctx, o.Bucket, d.Key, strings.NewReader(string(damaged)), int64(len(damaged)), minio.PutObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	third := testenv.EmptyPostgres(t)
	if _, err := Restore(ctx, s3, third, o, d.Key, nil); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("damaged dump: %v", err)
	}

	// pg_dump failing (no such database) leaves no object.
	u, _ := url.Parse(readerDSN)
	u.Path = "/no_such_database"
	if _, err := Run(ctx, s3, u.String(), o, at.Add(48*time.Hour)); err == nil || !strings.Contains(err.Error(), "pg_dump") {
		t.Fatalf("failed pg_dump: %v", err)
	}
	if all, _ := List(ctx, s3, o); len(all) != 2 {
		t.Fatalf("a failed dump left an object: %+v", all)
	}
}
