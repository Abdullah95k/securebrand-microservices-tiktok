// Package backup dumps schema tt to the object store and restores it (M16,
// H2; runbook docs/runbooks/tks-backup.md). A dump is pg_dump's custom
// format streamed straight into the backup bucket, never through a local
// file: the database only grows (D8), and the pods' disks are small. A dump
// whose pg_dump fails leaves no object behind. Next to each dump a small
// ".sha256" object holds its checksum, which a restore checks.
//
// tks never deletes a dump; the platform's bucket rules decide how long
// they are kept.
package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

// Options are where dumps go and the tools that make and read them.
type Options struct {
	Bucket string
	// Prefix starts every key ("tks/" in production); dumps live under
	// Prefix + "postgres/".
	Prefix string
	// PartMB is the size of each uploaded part, held in memory once; a
	// dump can be at most 10,000 parts (default 64: 640 GB).
	PartMB int
	// PgDump and PgRestore are the binaries (default from PATH). They must
	// be the server's major version or newer.
	PgDump, PgRestore string
}

func (o Options) dir() string { return o.Prefix + "postgres/" }

func (o Options) partSize() uint64 {
	if o.PartMB <= 0 {
		return 64 << 20
	}
	return uint64(o.PartMB) << 20
}

func bin(name, def string) string {
	if name != "" {
		return name
	}
	return def
}

// Dump is one stored dump.
type Dump struct {
	Key    string    `json:"key"`
	Bytes  int64     `json:"bytes"`
	At     time.Time `json:"at"`
	SHA256 string    `json:"sha256,omitempty"`
}

// Key names the dump taken at t: prefix/postgres/YYYY/MM/DD/tks-<UTC>.dump.
func Key(o Options, t time.Time) string {
	t = t.UTC()
	return o.dir() + t.Format("2006/01/02/") + "tks-" + t.Format("20060102T150405Z") + ".dump"
}

// conn splits a postgres URL into one without its password, for the
// command line, and the password, for PGPASSWORD.
func conn(dsn string) (string, string, error) {
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return "", "", errors.New("backup: postgres connection must be a postgres:// URL")
	}
	pw := ""
	if u.User != nil {
		pw, _ = u.User.Password()
		u.User = url.User(u.User.Username())
	}
	return u.String(), pw, nil
}

func command(ctx context.Context, name, dsn string, args ...string) (*exec.Cmd, error) {
	target, pw, err := conn(dsn)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, name, append(args, "--dbname", target)...) //nolint:gosec // G204: the operator's pg_dump or pg_restore, fixed arguments
	cmd.Env = append(os.Environ(), "PGPASSWORD="+pw, "PGAPPNAME=tksctl-backup")
	return cmd, nil
}

// tail keeps the last bytes a command wrote to stderr, for its error.
type tail struct{ b []byte }

func (t *tail) Write(p []byte) (int, error) {
	t.b = append(t.b, p...)
	if len(t.b) > 4096 {
		t.b = t.b[len(t.b)-4096:]
	}
	return len(p), nil
}

func (t *tail) String() string { return strings.TrimSpace(string(t.b)) }

// dumpReader is pg_dump's output, hashed as it is read, that ends with
// pg_dump's error instead of EOF when pg_dump fails, so the upload is
// aborted rather than completed with a truncated dump.
type dumpReader struct {
	out  io.Reader
	cmd  *exec.Cmd
	errs *tail
	sum  hash.Hash
	n    int64
	done bool
	err  error
}

func (r *dumpReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, r.err
	}
	n, err := r.out.Read(p)
	r.sum.Write(p[:n])
	r.n += int64(n)
	if errors.Is(err, io.EOF) {
		r.done, r.err = true, io.EOF
		if werr := r.cmd.Wait(); werr != nil {
			r.err = fmt.Errorf("backup: pg_dump: %w: %s", werr, r.errs)
		}
		return n, r.err
	}
	return n, err
}

// Run dumps schema tt from dsn into a new object and returns it.
func Run(ctx context.Context, s3 *minio.Client, dsn string, o Options, now time.Time) (Dump, error) {
	if o.Bucket == "" {
		return Dump{}, errors.New("backup: no bucket (s3.backup_bucket, S3_BUCKET_BACKUPS)")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// No owner and no rights in the dump: a restore belongs to whoever
	// restores it, and `tksctl migrate up` grants the roles afterwards.
	cmd, err := command(ctx, bin(o.PgDump, "pg_dump"), dsn, "--format=custom", "--schema=tt", "--no-owner", "--no-acl")
	if err != nil {
		return Dump{}, err
	}
	errs := &tail{}
	cmd.Stderr = errs
	out, err := cmd.StdoutPipe()
	if err != nil {
		return Dump{}, err
	}
	if err := cmd.Start(); err != nil {
		return Dump{}, fmt.Errorf("backup: start pg_dump: %w", err)
	}
	r := &dumpReader{out: out, cmd: cmd, errs: errs, sum: sha256.New()}
	d := Dump{Key: Key(o, now), At: now.UTC()}
	_, err = s3.PutObject(ctx, o.Bucket, d.Key, r, -1, minio.PutObjectOptions{
		ContentType: "application/octet-stream", PartSize: o.partSize(),
		// Unsigned payload rather than aws-chunked streaming signatures
		// (which some S3-compatible stores keep as part of the object): the
		// dump's own SHA-256 is checked at restore, and production's
		// endpoint is TLS.
		DisableContentSha256: true,
		UserMetadata:         map[string]string{"tks-format": "pg_dump-custom", "tks-schema": "tt"}})
	if err != nil {
		cancel()
		if !r.done {
			_ = cmd.Wait()
		}
		if r.err != nil && !errors.Is(r.err, io.EOF) {
			err = r.err
		}
		return Dump{}, fmt.Errorf("backup: upload %s: %w", d.Key, err)
	}
	if !r.done {
		return Dump{}, errors.New("backup: upload ended before pg_dump did")
	}
	d.Bytes, d.SHA256 = r.n, hex.EncodeToString(r.sum.Sum(nil))
	sum := d.SHA256 + "  " + d.Key[strings.LastIndexByte(d.Key, '/')+1:] + "\n"
	if _, err := s3.PutObject(ctx, o.Bucket, d.Key+".sha256", strings.NewReader(sum), int64(len(sum)),
		minio.PutObjectOptions{ContentType: "text/plain"}); err != nil {
		return Dump{}, fmt.Errorf("backup: checksum of %s: %w", d.Key, err)
	}
	return d, nil
}

var dumpName = regexp.MustCompile(`/tks-(\d{8}T\d{6}Z)\.dump$`)

// List returns the stored dumps, oldest first.
func List(ctx context.Context, s3 *minio.Client, o Options) ([]Dump, error) {
	var out []Dump
	for obj := range s3.ListObjects(ctx, o.Bucket, minio.ListObjectsOptions{Prefix: o.dir(), Recursive: true}) {
		if obj.Err != nil {
			return nil, fmt.Errorf("backup: list: %w", obj.Err)
		}
		m := dumpName.FindStringSubmatch(obj.Key)
		if m == nil {
			continue
		}
		at, err := time.Parse("20060102T150405Z", m[1])
		if err != nil {
			continue
		}
		out = append(out, Dump{Key: obj.Key, Bytes: obj.Size, At: at})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// Find returns the dump key names, or the newest for "latest".
func Find(ctx context.Context, s3 *minio.Client, o Options, key string) (Dump, error) {
	if key != "latest" {
		info, err := s3.StatObject(ctx, o.Bucket, key, minio.StatObjectOptions{})
		if err != nil {
			return Dump{}, fmt.Errorf("backup: %s: %w", key, err)
		}
		return Dump{Key: key, Bytes: info.Size, At: info.LastModified}, nil
	}
	all, err := List(ctx, s3, o)
	if err != nil {
		return Dump{}, err
	}
	if len(all) == 0 {
		return Dump{}, fmt.Errorf("backup: no dump under s3://%s/%s", o.Bucket, o.dir())
	}
	return all[len(all)-1], nil
}

// ErrNotEmpty means the target database already has schema tt: a restore
// goes into a new, empty database (the runbook says how to switch to it).
var ErrNotEmpty = errors.New("backup: the target database already has schema tt; restore into a new, empty database")

// Restore reads dump key into the database at dsn, which must have no
// schema tt yet, in one transaction (a failed restore leaves nothing), and
// checks the dump's checksum. The dsn's user owns what is restored; run
// `tksctl migrate up` against it afterwards to grant the roles.
func Restore(ctx context.Context, s3 *minio.Client, dsn string, o Options, key string, hasSchema func(context.Context) (bool, error)) (Dump, error) {
	d, err := Find(ctx, s3, o, key)
	if err != nil {
		return Dump{}, err
	}
	if hasSchema != nil {
		has, err := hasSchema(ctx)
		if err != nil {
			return Dump{}, err
		}
		if has {
			return Dump{}, ErrNotEmpty
		}
	}
	want := ""
	if obj, err := s3.GetObject(ctx, o.Bucket, d.Key+".sha256", minio.GetObjectOptions{}); err == nil {
		b, rerr := io.ReadAll(io.LimitReader(obj, 4096))
		_ = obj.Close()
		if rerr == nil {
			if f := strings.Fields(string(b)); len(f) > 0 {
				want = f[0]
			}
		}
	}
	obj, err := s3.GetObject(ctx, o.Bucket, d.Key, minio.GetObjectOptions{})
	if err != nil {
		return Dump{}, fmt.Errorf("backup: read %s: %w", d.Key, err)
	}
	defer obj.Close()
	cmd, err := command(ctx, bin(o.PgRestore, "pg_restore"), dsn, "--format=custom", "--no-owner", "--no-acl", "--exit-on-error", "--single-transaction")
	if err != nil {
		return Dump{}, err
	}
	sum := sha256.New()
	counted := &countReader{r: io.TeeReader(obj, sum)}
	cmd.Stdin = counted
	errs := &tail{}
	cmd.Stderr = errs
	if err := cmd.Run(); err != nil {
		return Dump{}, fmt.Errorf("backup: pg_restore %s: %w: %s", d.Key, err, errs)
	}
	// pg_restore may stop reading before the end (the table of contents
	// says it is done): hash the rest too.
	if _, err := io.Copy(io.Discard, counted); err != nil {
		return Dump{}, fmt.Errorf("backup: read %s: %w", d.Key, err)
	}
	d.Bytes, d.SHA256 = counted.n, hex.EncodeToString(sum.Sum(nil))
	if want != "" && want != d.SHA256 {
		return d, fmt.Errorf("backup: %s: checksum %s, want %s: the dump is damaged; restore another", d.Key, d.SHA256, want)
	}
	return d, nil
}

type countReader struct {
	r io.Reader
	n int64
}

func (c *countReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// ToolMajor returns the major version of a PostgreSQL client tool
// ("pg_dump (PostgreSQL) 18.1" is 18).
func ToolMajor(ctx context.Context, name string) (int, error) {
	out, err := exec.CommandContext(ctx, name, "--version").Output() //nolint:gosec // G204: the operator's client tool
	if err != nil {
		return 0, fmt.Errorf("backup: %s --version: %w", name, err)
	}
	f := strings.Fields(string(out))
	if len(f) == 0 {
		return 0, fmt.Errorf("backup: %s --version: %q", name, out)
	}
	v := f[len(f)-1]
	if i := strings.IndexAny(v, ".("); i > 0 {
		v = v[:i]
	}
	return strconv.Atoi(v)
}
