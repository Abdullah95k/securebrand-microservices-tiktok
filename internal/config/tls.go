package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
)

// TLS enables TLS to a server: the system roots plus CA (a PEM given as a
// value, as the production platform's Secret holds it) or CAFile when set,
// CertFile and KeyFile for mutual TLS, and ServerName to override the name
// verified in the server's certificate.
type TLS struct {
	Enabled    bool   `koanf:"enabled"`
	CA         string `koanf:"ca"`
	CAFile     string `koanf:"ca_file"`
	CertFile   string `koanf:"cert_file"`
	KeyFile    string `koanf:"key_file"`
	ServerName string `koanf:"server_name"`
}

// Config returns the tls.Config, or nil when TLS is disabled.
func (t TLS) Config() (*tls.Config, error) {
	if !t.Enabled {
		return nil, nil
	}
	c := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: t.ServerName}
	if t.CA != "" || t.CAFile != "" {
		pem := []byte(t.CA)
		if t.CAFile != "" {
			var err error
			if pem, err = os.ReadFile(t.CAFile); err != nil {
				return nil, fmt.Errorf("tls: ca_file: %w", err)
			}
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("tls: the CA holds no PEM certificate")
		}
		c.RootCAs = pool
	}
	if (t.CertFile == "") != (t.KeyFile == "") {
		return nil, errors.New("tls: cert_file and key_file go together")
	}
	if t.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("tls: client certificate: %w", err)
		}
		c.Certificates = []tls.Certificate{cert}
	}
	return c, nil
}
