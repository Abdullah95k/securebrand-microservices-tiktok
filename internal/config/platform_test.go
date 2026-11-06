package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func testCA(t *testing.T) string {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Now(), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &k.PublicKey, k)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

// The production platform's Secret (M18): credentials as separate values,
// CAs as PEM values, the SASL mechanism in capitals, a password with a
// comma, an object store given as a URL.
func TestPlatformCredentials(t *testing.T) {
	ca := testCA(t)
	t.Setenv("TKS_POSTGRES__HOST", "db.internal")
	t.Setenv("TKS_POSTGRES__PORT", "25060")
	t.Setenv("TKS_POSTGRES__DATABASE", "tks")
	t.Setenv("TKS_POSTGRES__USER", "tks_persister")
	t.Setenv("TKS_POSTGRES__PASSWORD", "p@ss,word/with:odd chars")
	t.Setenv("TKS_POSTGRES__READ_USER", "tks_reader")
	t.Setenv("TKS_POSTGRES__READ_PASSWORD", "r,o")
	t.Setenv("TKS_POSTGRES__SSLMODE", "require")
	t.Setenv("TKS_POSTGRES__CA", ca)
	t.Setenv("TKS_KAFKA__BROKERS", "b1:9093, b2:9093")
	t.Setenv("TKS_KAFKA__SASL__MECHANISM", "SCRAM-SHA-256")
	t.Setenv("TKS_KAFKA__SASL__USERNAME", "tks-kafka")
	t.Setenv("TKS_KAFKA__SASL__PASSWORD", "a,b,c")
	t.Setenv("TKS_KAFKA__TLS__ENABLED", "true")
	t.Setenv("TKS_KAFKA__TLS__CA", ca)
	t.Setenv("TKS_REDIS__TLS__ENABLED", "true")
	t.Setenv("TKS_REDIS__TLS__CA", ca)
	t.Setenv("TKS_REDIS__PASSWORD", "x,y")
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if c.Kafka.SASL.Mechanism != "scram-sha-256" || c.Kafka.SASL.Password != "a,b,c" || c.Redis.Password != "x,y" {
		t.Fatalf("kafka/redis: %+v %q", c.Kafka.SASL, c.Redis.Password)
	}
	if len(c.Kafka.Brokers) != 2 || c.Kafka.Brokers[1] != "b2:9093" {
		t.Fatalf("brokers %q", c.Kafka.Brokers)
	}
	for _, tl := range []TLS{c.Kafka.TLS, c.Redis.TLS} {
		tc, err := tl.Config()
		if err != nil || tc == nil || tc.RootCAs == nil {
			t.Fatalf("tls: %v %v", tc, err)
		}
	}
	raw, err := c.Postgres.URL()
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	pw, _ := u.User.Password()
	if u.Host != "db.internal:25060" || u.Path != "/tks" || u.User.Username() != "tks_persister" || pw != "p@ss,word/with:odd chars" ||
		u.Query().Get("sslmode") != "require" {
		t.Fatalf("url %s", raw)
	}
	b, err := os.ReadFile(u.Query().Get("sslrootcert"))
	if err != nil || string(b) != ca {
		t.Fatalf("ca file: %v", err)
	}
	read, _ := c.Postgres.ReadURL()
	if ru, _ := url.Parse(read); ru.User.Username() != "tks_reader" {
		t.Fatalf("read url %s", read)
	}
}

// Without a host the DSN is used as it is.
func TestPostgresDSN(t *testing.T) {
	p := Postgres{DSN: "postgres://a:b@h/d", ReadDSN: "postgres://r@h/d"}
	if u, _ := p.URL(); u != p.DSN {
		t.Fatal(u)
	}
	if u, _ := p.ReadURL(); u != p.ReadDSN {
		t.Fatal(u)
	}
	if err := (Postgres{Host: "h", User: "u"}).validate(); err == nil || !strings.Contains(err.Error(), "database") {
		t.Fatal(err)
	}
	if err := (Postgres{Host: "h", User: "u", Database: "d", SSLMode: "on"}).validate(); err == nil {
		t.Fatal("bad sslmode accepted")
	}
}

func TestTLSBadCA(t *testing.T) {
	if _, err := (TLS{Enabled: true, CA: "not a certificate"}).Config(); err == nil {
		t.Fatal("bad CA accepted")
	}
}
