package config

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// URL is the component's connection string: the DSN, or one built from
// Host and the other parts when Host is set (the production platform gives
// its credentials as separate values).
func (p Postgres) URL() (string, error) {
	return p.url(p.User, p.Password, p.DSN)
}

// ReadURL is the API's read pool: ReadDSN, or the server's parts with
// ReadUser; "" when neither is set (reads use the main pool).
func (p Postgres) ReadURL() (string, error) {
	if p.Host != "" && p.ReadUser != "" {
		return p.url(p.ReadUser, p.ReadPassword, "")
	}
	return p.ReadDSN, nil
}

func (p Postgres) url(user, password, dsn string) (string, error) {
	if p.Host == "" {
		return dsn, nil
	}
	port := p.Port
	if port == 0 {
		port = 5432
	}
	u := url.URL{Scheme: "postgres", User: url.UserPassword(user, password),
		Host: net.JoinHostPort(p.Host, strconv.Itoa(port)), Path: "/" + p.Database}
	q := url.Values{}
	if p.SSLMode != "" {
		q.Set("sslmode", p.SSLMode)
	}
	ca := p.CAFile
	if p.CA != "" {
		var err error
		if ca, err = caFile(p.CA); err != nil {
			return "", err
		}
	}
	if ca != "" {
		q.Set("sslrootcert", ca)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (p Postgres) validate() error {
	if p.Host == "" {
		return nil
	}
	if p.Database == "" || p.User == "" {
		return errors.New("postgres.host needs postgres.database and postgres.user")
	}
	switch p.SSLMode {
	case "", "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
	default:
		return fmt.Errorf("postgres.sslmode %q: want disable, allow, prefer, require, verify-ca or verify-full", p.SSLMode)
	}
	return nil
}

var caFiles sync.Map // PEM hash -> path

// caFile writes a PEM given as a value to a file once (pgx reads the CA of
// sslrootcert from a file) under the temporary directory, which the
// chart's pods mount writable.
func caFile(pem string) (string, error) {
	sum := sha256.Sum256([]byte(pem))
	key := hex.EncodeToString(sum[:8])
	if v, ok := caFiles.Load(key); ok {
		return v.(string), nil
	}
	path := filepath.Join(os.TempDir(), "tks-postgres-ca-"+key+".pem")
	if err := os.WriteFile(path, []byte(pem), 0o600); err != nil {
		return "", fmt.Errorf("postgres.ca: %w", err)
	}
	caFiles.Store(key, path)
	return path, nil
}
