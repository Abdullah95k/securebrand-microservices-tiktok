package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/config"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/platform/s3x"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/backup"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/iraq"
	"github.com/abdullah95k/securebrand-microservices-tiktok/internal/tt/ttstore"
)

// backupFlags are the options every backup command takes.
func backupFlags(fs *flag.FlagSet, cfg *config.Config) *backup.Options {
	o := &backup.Options{}
	fs.StringVar(&o.Bucket, "bucket", cfg.S3.BackupBucket, "bucket of the dumps (s3.backup_bucket)")
	fs.StringVar(&o.Prefix, "prefix", cfg.S3.BackupPrefix, "key prefix; dumps are under <prefix>postgres/ (s3.backup_prefix)")
	fs.IntVar(&o.PartMB, "part-mb", 64, "upload part size in MB, held in memory; a dump is at most 10,000 parts")
	fs.StringVar(&o.PgDump, "pg-dump", "pg_dump", "pg_dump binary (the server's major version or newer)")
	fs.StringVar(&o.PgRestore, "pg-restore", "pg_restore", "pg_restore binary")
	return o
}

// backupOut is what the backup commands print: a dump, with its time in
// Iraq time for people.
type backupOut struct {
	backup.Dump
	AtIraq string `json:"at_iraq"`
}

func dumpOut(d backup.Dump) backupOut { return backupOut{Dump: d, AtIraq: iraq.Format(d.At)} }

// runBackup is `tksctl backup run|list|restore` (M16; runbook
// docs/runbooks/tks-backup.md).
func runBackup(ctx context.Context, cfg *config.Config, sub string, args []string) (any, error) {
	fs := flag.NewFlagSet("backup "+sub, flag.ExitOnError)
	o := backupFlags(fs, cfg)
	key := fs.String("key", "latest", "restore: the dump's key, or latest")
	target := fs.String("target-database", "", "restore: the new, empty database to restore into, on the same server (required)")
	_ = fs.Parse(args)
	if o.Bucket == "" {
		return nil, errors.New("backup: no bucket: set s3.backup_bucket (S3_BUCKET_BACKUPS) or --bucket")
	}
	s3, err := s3x.Connect(cfg.S3.Endpoint, cfg.S3.AccessKey, cfg.S3.SecretKey, cfg.S3.Region, cfg.S3.UseSSL)
	if err != nil {
		return nil, err
	}
	switch sub {
	case "run":
		dsn, err := cfg.Postgres.URL()
		if err != nil {
			return nil, err
		}
		if err := checkTool(ctx, cfg, o.PgDump); err != nil {
			return nil, err
		}
		d, err := backup.Run(ctx, s3, dsn, *o, time.Now())
		if err != nil {
			return nil, err
		}
		return dumpOut(d), nil
	case "list":
		all, err := backup.List(ctx, s3, *o)
		if err != nil {
			return nil, err
		}
		out := make([]backupOut, 0, len(all))
		for _, d := range all {
			out = append(out, dumpOut(d))
		}
		return out, nil
	case "restore":
		if *target == "" {
			return nil, errors.New("backup restore: --target-database is required (a new, empty database; never the live one)")
		}
		dsn, err := targetURL(cfg.Postgres, *target)
		if err != nil {
			return nil, err
		}
		if err := checkTool(ctx, cfg, o.PgRestore); err != nil {
			return nil, err
		}
		hasSchema := func(ctx context.Context) (bool, error) {
			db, err := pgxpool.New(ctx, dsn)
			if err != nil {
				return false, err
			}
			defer db.Close()
			var has bool
			err = db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)`, ttstore.Schema).Scan(&has)
			return has, err
		}
		d, err := backup.Restore(ctx, s3, dsn, *o, *key, hasSchema)
		if err != nil {
			return nil, err
		}
		return dumpOut(d), nil
	}
	return nil, fmt.Errorf("backup %s: want run, list or restore", sub)
}

// targetURL is the connection to database name on the configured server,
// as the configured user.
func targetURL(p config.Postgres, name string) (string, error) {
	if p.Host != "" {
		p.Database = name
		return p.URL()
	}
	u, err := url.Parse(p.DSN)
	if err != nil || u.Scheme == "" {
		return "", errors.New("backup restore: postgres.dsn must be a postgres:// URL")
	}
	u.Path = "/" + name
	return u.String(), nil
}

// checkTool refuses a client tool older than the server: pg_dump would
// refuse the server anyway, after the upload started.
func checkTool(ctx context.Context, cfg *config.Config, tool string) error {
	major, err := backup.ToolMajor(ctx, tool)
	if err != nil {
		return err
	}
	db, err := ttstore.OpenConfig(ctx, cfg.Postgres, 1)
	if err != nil {
		return err
	}
	defer db.Close()
	var server int
	if err := db.QueryRow(ctx, `SELECT current_setting('server_version_num')::int / 10000`).Scan(&server); err != nil {
		return err
	}
	if major < server {
		return fmt.Errorf("backup: %s is version %d, the server %d: use the backup image (deploy/docker/Dockerfile --target backup)", tool, major, server)
	}
	return nil
}
