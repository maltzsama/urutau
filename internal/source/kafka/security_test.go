package kafka

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maltzsama/urutau/spec"
)

func TestSASLMechanism(t *testing.T) {
	for _, mech := range []string{"", "plain", "scram-sha-256", "scram-sha-512"} {
		m, err := saslMechanism(&spec.KafkaSASL{Mechanism: mech, Username: "u", Password: "p"})
		if err != nil || m == nil {
			t.Fatalf("mechanism %q: mechanism=%v err=%v", mech, m, err)
		}
	}
	if _, err := saslMechanism(&spec.KafkaSASL{Mechanism: "aws-iam"}); err == nil {
		t.Fatal("an unsupported mechanism must error, not silently fall back")
	}
}

func TestSecurityOptsNilIsNoop(t *testing.T) {
	opts, err := securityOpts(spec.Source{})
	if err != nil || opts != nil {
		t.Fatalf("no kafka block: opts=%v err=%v, want nil,nil", opts, err)
	}
}

func TestTLSConfigFromFiles(t *testing.T) {
	caPath, certPath, keyPath := writeSelfSigned(t)

	cfg, err := tlsConfig(&spec.KafkaTLS{CA: caPath, Cert: certPath, Key: keyPath})
	if err != nil {
		t.Fatalf("tlsConfig: %v", err)
	}
	if cfg.RootCAs == nil {
		t.Fatal("CA pool was not loaded")
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("client certs = %d, want 1", len(cfg.Certificates))
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Fatalf("MinVersion = %x, want TLS 1.2", cfg.MinVersion)
	}

	if _, err := tlsConfig(&spec.KafkaTLS{CA: filepath.Join(t.TempDir(), "missing.pem")}); err == nil {
		t.Fatal("a missing CA file must error")
	}
}

// writeSelfSigned writes a self-signed cert/key pair and its CA (the same
// cert) to temp files, returning the three paths.
func writeSelfSigned(t *testing.T) (caPath, certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kafka-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	caPath = filepath.Join(dir, "ca.pem")
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	write := func(path, typ string, bytes []byte) {
		if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: bytes}), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(caPath, "CERTIFICATE", der)
	write(certPath, "CERTIFICATE", der)
	write(keyPath, "EC PRIVATE KEY", keyDER)
	return caPath, certPath, keyPath
}
