package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
)

// TLS enables TLS to a server: the system roots plus CAFile when set,
// CertFile and KeyFile for mutual TLS, and ServerName to override the name
// verified in the server's certificate.
type TLS struct {
	Enabled    bool   `koanf:"enabled"`
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
	if t.CAFile != "" {
		pem, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("tls: ca_file: %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("tls: ca_file %s holds no PEM certificate", t.CAFile)
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
