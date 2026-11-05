package accounts

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/scrape/ratelimit"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

type fixture struct {
	db   *pgxpool.Pool
	cfg  *config.Config
	a, b uuid.UUID
	now  time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	db, err := ttstore.Open(ctx, testenv.EmptyPostgres(t), 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := ttstore.MigrateUp(ctx, db); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{db: db, cfg: cfg, a: uuid.New(), b: uuid.New(), now: time.Now().UTC().Truncate(time.Second)}
	for _, id := range []uuid.UUID{f.a, f.b} {
		if _, err := db.Exec(ctx, `INSERT INTO api_clients (id, name, key_prefix, key_hash, scopes) VALUES ($1, $2, $2, '\x00', '{accounts:write}')`,
			id, id.String()[:8]); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func (f *fixture) one(t *testing.T, q string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := f.db.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

func (f *fixture) importText(t *testing.T, client uuid.UUID, text string, o ImportOptions) *Report {
	t.Helper()
	lines, err := Parse(strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	o.ClientID = client
	if o.Now.IsZero() {
		o.Now = f.now
	}
	rep, err := Import(context.Background(), f.db, f.cfg, lines, o)
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

// An import registers each account once, reports every line, labels them,
// records the batch as their source, and changes nothing when dry; running
// it again is safe.
func TestImport(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	// Client b already monitors "shared".
	if _, err := Register(ctx, f.db, Registration{ClientID: f.b, Ref: "u:shared", How: HowAPI, Origin: OriginOwner,
		Priority: PriorityOwner, DefaultInterval: time.Hour, Enabled: true, Now: f.now}); err != nil {
		t.Fatal(err)
	}
	text := "first\n@First\nhttps://www.tiktok.com/@second?lang=en\nshared\nnot valid!\nhttps://vm.tiktok.com/x/\nid:12345\n"
	dry := f.importText(t, f.a, text, ImportOptions{Labels: []string{"list:banks"}, DryRun: true})
	if dry.Added != 2 || dry.Joined != 1 || dry.Duplicates != 1 || len(dry.Rejected) != 3 || dry.Lines != 7 || dry.Queued != 3 {
		t.Fatalf("dry run: %+v", dry)
	}
	if n := f.one(t, `SELECT count(*) FROM account_schedule`); n != 1 {
		t.Fatalf("the dry run wrote %d schedules", n)
	}
	if n := f.one(t, `SELECT count(*) FROM account_registrations WHERE api_client_id = $1`, f.a); n != 0 {
		t.Fatalf("the dry run registered %d", n)
	}
	rep := f.importText(t, f.a, text, ImportOptions{Labels: []string{"list:banks"}})
	if rep.Added != 2 || rep.Joined != 1 || rep.Already != 0 || rep.Duplicates != 1 || len(rep.Rejected) != 3 {
		t.Fatalf("import: %+v", rep)
	}
	reasons := map[int]string{}
	for _, r := range rep.Rejected {
		reasons[r.Line] = r.Reason
	}
	if reasons[5] != ReasonInvalid || reasons[6] != ReasonShortLink || !strings.Contains(reasons[7], "id: reference") {
		t.Errorf("reasons: %v", reasons)
	}
	if n := f.one(t, `SELECT count(*) FROM account_schedule`); n != 3 {
		t.Errorf("%d schedules, want 3 (one shared)", n)
	}
	if n := f.one(t, `SELECT count(*) FROM account_labels WHERE api_client_id = $1 AND label = 'list:banks'`, f.a); n != 3 {
		t.Errorf("%d labelled, want 3", n)
	}
	if n := f.one(t, `SELECT count(*) FROM account_sources WHERE how = 'import' AND via = $1`, rep.Batch.String()); n != 3 {
		t.Errorf("%d import sources, want 3", n)
	}
	if n := f.one(t, `SELECT count(*) FROM import_lines WHERE batch_id = $1`, rep.Batch); n != 7 {
		t.Errorf("%d lines recorded, want 7", n)
	}
	// The new schedules wait for admission, at the owner's priority; the
	// shared one too, since tks has not read it yet.
	if n := f.one(t, `SELECT count(*) FROM account_schedule WHERE admitted_at IS NULL AND origin = 'owner' AND admit_priority = $1`,
		PriorityOwner); n != 3 {
		t.Errorf("%d queued owner schedules, want 3", n)
	}
	// Again, with another label: nothing new, the new label added.
	again := f.importText(t, f.a, text, ImportOptions{Labels: []string{"tier:1"}})
	if again.Added != 0 || again.Joined != 0 || again.Already != 3 {
		t.Errorf("second run: %+v", again)
	}
	if n := f.one(t, `SELECT count(*) FROM account_labels WHERE api_client_id = $1`, f.a); n != 6 {
		t.Errorf("%d labels, want 6", n)
	}
	if n := f.one(t, `SELECT sum(seen) FROM account_sources WHERE account_ref = 'u:first'`); n != 2 {
		t.Errorf("the first account seen %d times, want 2 (two batches)", n)
	}
}

// --disable-missing removes the client's other registrations, refusing
// more than 10 % without --force; the data and other clients' schedules
// stay.
func TestImportDisableMissing(t *testing.T) {
	f := newFixture(t)
	var list []string
	for i := range 20 {
		list = append(list, fmt.Sprintf("acct%02d", i))
	}
	f.importText(t, f.a, strings.Join(list, "\n"), ImportOptions{})
	f.importText(t, f.b, "acct00\n", ImportOptions{})
	// Dropping 1 of 20 (5 %) is allowed.
	rep := f.importText(t, f.a, strings.Join(list[1:], "\n"), ImportOptions{DisableMissing: true, DryRun: true})
	if d := rep.Disabled; d == nil || d.Missing != 1 || d.Removed != 0 || d.Refused != "" || d.Refs[0] != "u:acct00" {
		t.Fatalf("dry: %+v", rep.Disabled)
	}
	rep = f.importText(t, f.a, strings.Join(list[1:], "\n"), ImportOptions{DisableMissing: true})
	if d := rep.Disabled; d.Removed != 1 {
		t.Fatalf("applied: %+v", d)
	}
	// acct00 stays scheduled: b is still registered.
	if n := f.one(t, `SELECT count(*) FROM account_schedule WHERE account_ref = 'u:acct00' AND enabled`); n != 1 {
		t.Error("another client's schedule was disabled")
	}
	// Dropping 5 of 19 (26 %) needs --force.
	lines, _ := Parse(strings.NewReader(strings.Join(list[6:], "\n")))
	_, err := Import(context.Background(), f.db, f.cfg, lines, ImportOptions{ClientID: f.a, DisableMissing: true, Now: f.now})
	if !errors.Is(err, ErrTooManyToDisable) {
		t.Fatalf("26 %% removed without --force: %v", err)
	}
	if n := f.one(t, `SELECT count(*) FROM account_registrations WHERE api_client_id = $1`, f.a); n != 19 {
		t.Errorf("a refused run removed registrations: %d left", n)
	}
	rep = f.importText(t, f.a, strings.Join(list[6:], "\n"), ImportOptions{DisableMissing: true, Force: true})
	if rep.Disabled.Removed != 5 {
		t.Errorf("forced: %+v", rep.Disabled)
	}
	if n := f.one(t, `SELECT count(*) FROM account_schedule WHERE NOT enabled`); n != 5 {
		t.Errorf("%d schedules disabled, want 5 (no client left)", n)
	}
}

// The report of an import tells what checks found since.
func TestBatchReport(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	rep := f.importText(t, f.a, "queued\nprivate\nquiet\nactive\nunread\n", ImportOptions{})
	_, _ = f.db.Exec(ctx, `UPDATE account_schedule SET admitted_at = $1 WHERE account_ref <> 'u:queued'`, f.now)
	for i, name := range []string{"private", "quiet", "active"} {
		uid := int64(100 + i)
		status := "active"
		if name == "private" {
			status = "private"
		}
		if _, err := f.db.Exec(ctx, `INSERT INTO accounts (user_id, sec_uid, username, author_key, status) VALUES ($1, $2, $3, '\x00', $4)`,
			uid, "sec"+name, name, status); err != nil {
			t.Fatal(err)
		}
		_, _ = f.db.Exec(ctx, `UPDATE account_schedule SET user_id = $1 WHERE account_ref = $2`, uid, "u:"+name)
		_, _ = f.db.Exec(ctx, `INSERT INTO crawl_cursors (scope_type, scope_id, state) VALUES ('account_list', $1, 'complete')`, uid)
	}
	_, _ = f.db.Exec(ctx, `UPDATE account_schedule SET last_post_at = $1 WHERE account_ref = 'u:active'`, f.now.Add(-24*time.Hour))
	_, _ = f.db.Exec(ctx, `UPDATE account_schedule SET last_post_at = $1 WHERE account_ref = 'u:quiet'`, f.now.Add(-100*24*time.Hour))
	all, err := BatchReport(ctx, f.db, f.a, false, rep.Batch, false, f.now)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, b := range all {
		got[b.AccountRef] = b.Status
	}
	want := map[string]string{"u:queued": "queued", "u:private": "private", "u:quiet": "inactive", "u:active": "active", "u:unread": "unread"}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: %q, want %q", k, got[k], v)
		}
	}
	problems, _ := BatchReport(ctx, f.db, f.a, false, rep.Batch, true, f.now)
	if len(problems) != 2 {
		t.Errorf("problems: %+v", problems)
	}
	if _, err := BatchReport(ctx, f.db, f.b, false, rep.Batch, false, f.now); err == nil {
		t.Error("another client read the batch")
	}
}

// Admission lets the owner's accounts start first, then discovered ones by
// priority, at most per_hour an hour; an account whose list tks has read is
// admitted at registration and does not count.
func TestAdmit(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.cfg.TT.Admission.PerHour = 3
	reg := func(ref, origin string, prio int, at time.Time) {
		t.Helper()
		if _, err := Register(ctx, f.db, Registration{ClientID: f.a, Ref: ref, How: HowAPI, Origin: origin, Priority: prio,
			DefaultInterval: time.Hour, Enabled: true, Now: at}); err != nil {
			t.Fatal(err)
		}
	}
	reg("u:disc_low", OriginDiscovery, PriorityDiscovered+10, f.now.Add(-3*time.Hour))
	reg("u:disc_high", OriginDiscovery, PriorityDiscovered+90, f.now.Add(-2*time.Hour))
	reg("u:owner_old", OriginOwner, PriorityOwner, f.now.Add(-time.Hour))
	reg("u:owner_new", OriginOwner, PriorityOwner, f.now)
	// A read account: admitted at once.
	if _, err := f.db.Exec(ctx, `INSERT INTO accounts (user_id, sec_uid, username, author_key) VALUES (5, 'sec5', 'known', '\x00')`); err != nil {
		t.Fatal(err)
	}
	_, _ = f.db.Exec(ctx, `INSERT INTO account_usernames VALUES (5, 'known', now(), now())`)
	_, _ = f.db.Exec(ctx, `INSERT INTO crawl_cursors (scope_type, scope_id, state) VALUES ('account_list', 5, 'complete')`)
	res, err := Register(ctx, f.db, Registration{ClientID: f.a, Ref: "u:known", How: HowAPI, Origin: OriginOwner, Priority: PriorityOwner,
		DefaultInterval: time.Hour, Enabled: true, Now: f.now})
	if err != nil || res.Queued {
		t.Fatalf("a read account queued: %+v %v", res, err)
	}
	n, err := Admit(ctx, f.db, f.cfg, nil, f.now, 100)
	if err != nil || n != 3 {
		t.Fatalf("admitted %d (%v), want 3", n, err)
	}
	waiting := f.one(t, `SELECT count(*) FROM account_schedule WHERE admitted_at IS NULL`)
	var left string
	_ = f.db.QueryRow(ctx, `SELECT account_ref FROM account_schedule WHERE admitted_at IS NULL`).Scan(&left)
	if waiting != 1 || left != "u:disc_low" {
		t.Errorf("left waiting: %d %s, want the low-score discovered account", waiting, left)
	}
	// The hour is used up; an hour later the rest goes.
	if n, _ := Admit(ctx, f.db, f.cfg, nil, f.now.Add(time.Minute), 100); n != 0 {
		t.Errorf("admitted %d more within the hour", n)
	}
	if n, _ := Admit(ctx, f.db, f.cfg, nil, f.now.Add(61*time.Minute), 100); n != 1 {
		t.Errorf("admitted %d an hour later, want 1", n)
	}
	q, err := QueueStatus(ctx, f.db, f.cfg, nil, f.now)
	if err != nil || q.Waiting != 0 || q.PerHour != 3 || q.Unlimited {
		t.Errorf("queue: %+v %v", q, err)
	}
}

func TestRate(t *testing.T) {
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC) // 12 h left in the budget's day
	if n, unl := Rate(cfg, nil, now); !unl {
		t.Errorf("no budget: %d, want unlimited", n)
	}
	cfg.Scheduler.Budget.Enabled, cfg.Scheduler.Budget.RequestsPerDay = true, 2_400_000
	cfg.TT.Admission.RequestsPerAccount = 1000
	if n, unl := Rate(cfg, nil, now); unl || n != 100 {
		t.Errorf("budget, state unknown: %d %v, want 100 (the day evenly)", n, unl)
	}
	st := &ratelimit.BudgetState{UpdatedAt: now, Projected: [2]int64{1_200_000, 0}}
	if n, _ := Rate(cfg, st, now); n != 100 {
		t.Errorf("1.2 M left over 12 h: %d, want 100", n)
	}
	st.Projected[0] = 3_000_000
	if n, _ := Rate(cfg, st, now); n != 0 {
		t.Errorf("projected over budget: %d, want 0", n)
	}
	st.Projected[0], st.PauseP2 = 0, true
	if n, _ := Rate(cfg, st, now); n != 0 {
		t.Errorf("backfills paused: %d, want 0", n)
	}
	cfg.TT.Admission.PerHour = 7
	if n, unl := Rate(cfg, st, now); n != 0 || unl {
		t.Errorf("per_hour, backfills paused: %d %v, want 0", n, unl)
	}
	st.PauseP2 = false
	if n, unl := Rate(cfg, st, now); n != 7 || unl {
		t.Errorf("per_hour: %d %v", n, unl)
	}
	// Paused by the dollar budget alone (no request budget): none either.
	cfg.TT.Admission.PerHour, cfg.Scheduler.Budget.RequestsPerDay = 0, 0
	if n, unl := Rate(cfg, &ratelimit.BudgetState{UpdatedAt: now, PauseP2: true}, now); n != 0 || unl {
		t.Errorf("dollar budget paused: %d %v, want 0", n, unl)
	}
	c := Needs(cfg.TT.Admission, 10_000)
	if c.PerHour != 417 || c.Requests != 10_000_000 || c.IPs != 30 || c.SignerPages != 30 {
		t.Errorf("needs: %+v", c)
	}
}

// Blocks: a removal blocks the account for discovery; the same client's
// registration lifts it.
func TestBlocks(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if err := Block(ctx, f.db, f.a, "u:gone", 0, f.now); err != nil {
		t.Fatal(err)
	}
	if b, _ := Blocked(ctx, f.db, "u:gone", 0); !b {
		t.Fatal("not blocked")
	}
	if _, err := Register(ctx, f.db, Registration{ClientID: f.b, Ref: "u:gone", How: HowAPI, Origin: OriginOwner,
		DefaultInterval: time.Hour, Enabled: true, Now: f.now}); err != nil {
		t.Fatal(err)
	}
	if b, _ := Blocked(ctx, f.db, "u:gone", 0); !b {
		t.Error("another client's registration lifted the block")
	}
	if _, err := Register(ctx, f.db, Registration{ClientID: f.a, Ref: "u:gone", How: HowAPI, Origin: OriginOwner,
		DefaultInterval: time.Hour, Enabled: true, Now: f.now}); err != nil {
		t.Fatal(err)
	}
	if b, _ := Blocked(ctx, f.db, "u:gone", 0); b {
		t.Error("the client's own registration did not lift its block")
	}
}
