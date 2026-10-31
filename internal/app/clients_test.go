package app

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
)

func TestKafkaClientSecurity(t *testing.T) {
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	kc, err := KafkaClient(cfg, "-x")
	if err != nil || kc.TLS != nil || kc.SASL != nil || kc.ClientID != "fbs-x" {
		t.Fatalf("plain client: %+v, %v", kc, err)
	}
	for mech, name := range map[string]string{"plain": "PLAIN", "scram-sha-256": "SCRAM-SHA-256", "scram-sha-512": "SCRAM-SHA-512"} {
		cfg.Kafka.SASL = config.SASL{Mechanism: mech, Username: "u", Password: "p"}
		cfg.Kafka.TLS = config.TLS{Enabled: true, ServerName: "kafka.internal"}
		kc, err := KafkaClient(cfg, "")
		if err != nil || kc.SASL == nil || kc.SASL.Name() != name || kc.TLS == nil || kc.TLS.ServerName != "kafka.internal" {
			t.Fatalf("%s: %+v, %v", mech, kc, err)
		}
	}
	cfg.Kafka.TLS = config.TLS{Enabled: true, CAFile: filepath.Join(t.TempDir(), "missing.pem")}
	if _, err := KafkaClient(cfg, ""); err == nil {
		t.Fatal("a missing CA file must fail")
	}
}

func TestTLSConfigFiles(t *testing.T) {
	dir := t.TempDir()
	ca, key := selfSigned(t, dir, "ca")
	if c, err := (config.TLS{}).Config(); c != nil || err != nil {
		t.Fatalf("disabled: %v, %v", c, err)
	}
	c, err := config.TLS{Enabled: true, CAFile: ca}.Config()
	if err != nil || c.RootCAs == nil {
		t.Fatalf("with CA: %v, %v", c, err)
	}
	if _, err := (config.TLS{Enabled: true, CertFile: ca}).Config(); err == nil {
		t.Fatal("a certificate without its key must fail")
	}
	c, err = config.TLS{Enabled: true, CertFile: ca, KeyFile: key}.Config()
	if err != nil || len(c.Certificates) != 1 {
		t.Fatalf("client certificate: %v, %v", c, err)
	}
	bad := filepath.Join(dir, "bad.pem")
	if err := os.WriteFile(bad, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := (config.TLS{Enabled: true, CAFile: bad}).Config(); err == nil {
		t.Fatal("a CA file without PEM must fail")
	}
}

// TestSecureEndpoints produces and consumes through a Kafka that needs TLS
// and SASL, and talks to a Valkey over TLS, when they are given:
// IGS_TEST_SECURE_KAFKA (brokers), IGS_TEST_SECURE_SASL (mechanism:user:pass),
// IGS_TEST_SECURE_REDIS (addr) and IGS_TEST_SECURE_CA (the CA of both).
func TestSecureEndpoints(t *testing.T) {
	brokers, ca := os.Getenv("IGS_TEST_SECURE_KAFKA"), os.Getenv("IGS_TEST_SECURE_CA")
	if brokers == "" || ca == "" {
		t.Skip("IGS_TEST_SECURE_KAFKA / IGS_TEST_SECURE_CA not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Kafka.Brokers = strings.Split(brokers, ",")
	cfg.Kafka.TLS = config.TLS{Enabled: true, CAFile: ca}
	parts := strings.SplitN(os.Getenv("IGS_TEST_SECURE_SASL"), ":", 3)
	if len(parts) != 3 {
		t.Fatal("IGS_TEST_SECURE_SASL must be mechanism:user:pass")
	}
	cfg.Kafka.SASL = config.SASL{Mechanism: parts[0], Username: parts[1], Password: parts[2]}
	kc, err := KafkaClient(cfg, "-securetest")
	if err != nil {
		t.Fatal(err)
	}
	topic := "fbs-secure-test-" + time.Now().Format("150405.000")
	cl, err := kgo.NewClient(append(kc.Opts(), kgo.AllowAutoTopicCreation(), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))...)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("hello")}).FirstErr(); err != nil {
		t.Fatalf("produce over TLS+SASL: %v", err)
	}
	fs := cl.PollFetches(ctx)
	if err := fs.Err(); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if recs := fs.Records(); len(recs) != 1 || string(recs[0].Value) != "hello" {
		t.Fatalf("consumed %v", recs)
	}
	// A client without the credentials is refused.
	plain := kc
	plain.SASL = nil
	nocreds, _ := kgo.NewClient(append(plain.Opts(), kgo.AllowAutoTopicCreation())...)
	defer nocreds.Close()
	short, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	if err := nocreds.ProduceSync(short, &kgo.Record{Topic: topic, Value: []byte("x")}).FirstErr(); err == nil {
		t.Fatal("a client without SASL credentials produced")
	}

	if addr := os.Getenv("IGS_TEST_SECURE_REDIS"); addr != "" {
		cfg.Redis.Addr, cfg.Redis.TLS = addr, config.TLS{Enabled: true, CAFile: ca}
		rdb, err := OpenRedis(ctx, cfg)
		if err != nil {
			t.Fatalf("redis over TLS: %v", err)
		}
		defer func() { _ = rdb.Close() }()
		if err := rdb.Set(ctx, "fbs-secure-test", "ok", time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		cfg.Redis.TLS = config.TLS{}
		if _, err := OpenRedis(ctx, cfg); err == nil {
			t.Fatal("a plain connection to the TLS port worked")
		}
	}
}

// selfSigned writes a self-signed certificate and key; it returns their paths.
func selfSigned(t *testing.T, dir, name string) (certFile, keyFile string) {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &k.PublicKey, k)
	if err != nil {
		t.Fatal(err)
	}
	kb, err := x509.MarshalECPrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile = filepath.Join(dir, name+".pem"), filepath.Join(dir, name+"-key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}
