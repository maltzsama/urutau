package mysql

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
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseURI(t *testing.T) {
	tests := []struct {
		name    string
		uri     string
		want    *URI
		wantErr bool
	}{
		{
			name: "full uri",
			uri:  "mysql://root:secret@localhost:3306/mydb",
			want: &URI{User: "root", Password: "secret", Host: "localhost", Port: "3306", DB: "mydb"},
		},
		{
			name: "no port defaults to 3306",
			uri:  "mysql://root:secret@localhost/mydb",
			want: &URI{User: "root", Password: "secret", Host: "localhost", Port: "3306", DB: "mydb"},
		},
		{
			name: "no password",
			uri:  "mysql://root@localhost/mydb",
			want: &URI{User: "root", Password: "", Host: "localhost", Port: "3306", DB: "mydb"},
		},
		{
			name:    "wrong scheme",
			uri:     "postgres://root@localhost/mydb",
			wantErr: true,
		},
		{
			name:    "no host",
			uri:     "mysql://root@/mydb",
			wantErr: true,
		},
		{
			name:    "no db",
			uri:     "mysql://root@localhost/",
			wantErr: true,
		},
		{
			name:    "empty uri",
			uri:     "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseURI(tt.uri)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseURI(%q) error = %v, wantErr %v", tt.uri, err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if got.User != tt.want.User || got.Password != tt.want.Password || got.Host != tt.want.Host || got.Port != tt.want.Port || got.DB != tt.want.DB {
				t.Fatalf("ParseURI(%q) = %+v, want %+v", tt.uri, got, tt.want)
			}
		})
	}
}

func TestURIAddr(t *testing.T) {
	u := &URI{Host: "localhost", Port: "3306"}
	if got := u.Addr(); got != "localhost:3306" {
		t.Fatalf("Addr() = %q, want localhost:3306", got)
	}
}

func TestURIQueryDSN(t *testing.T) {
	u := &URI{User: "root", Password: "secret", Host: "localhost", Port: "3306", DB: "mydb"}
	want := "root:secret@tcp(localhost:3306)/mydb?parseTime=true&loc=UTC&time_zone=" +
		url.QueryEscape("'+00:00'")
	got, err := u.QueryDSN()
	if err != nil {
		t.Fatalf("QueryDSN: %v", err)
	}
	if got != want {
		t.Fatalf("QueryDSN() = %q, want %q", got, want)
	}
}

func TestParseURITLSModes(t *testing.T) {
	// Off by default.
	u, err := ParseURI("mysql://root@localhost/mydb")
	if err != nil {
		t.Fatalf("ParseURI: %v", err)
	}
	if u.TLSConfig() != nil {
		t.Fatal("TLS must be off by default")
	}

	// true: verify against system roots + hostname.
	u, err = ParseURI("mysql://root@db.example.com/mydb?tls=true")
	if err != nil {
		t.Fatalf("ParseURI(tls=true): %v", err)
	}
	if cfg := u.TLSConfig(); cfg == nil || cfg.ServerName != "db.example.com" || cfg.InsecureSkipVerify {
		t.Fatalf("tls=true config = %+v", cfg)
	}

	// verify-ca: chain verified, hostname skipped (custom VerifyConnection).
	u, err = ParseURI("mysql://root@db.example.com/mydb?tls=verify-ca")
	if err != nil {
		t.Fatalf("ParseURI(tls=verify-ca): %v", err)
	}
	if cfg := u.TLSConfig(); cfg == nil || !cfg.InsecureSkipVerify || cfg.VerifyConnection == nil {
		t.Fatalf("tls=verify-ca config = %+v", cfg)
	}

	// skip-verify.
	u, err = ParseURI("mysql://root@db.example.com/mydb?tls=skip-verify")
	if err != nil {
		t.Fatalf("ParseURI(tls=skip-verify): %v", err)
	}
	if cfg := u.TLSConfig(); cfg == nil || !cfg.InsecureSkipVerify || cfg.VerifyConnection != nil {
		t.Fatalf("tls=skip-verify config = %+v", cfg)
	}

	// verify-full honours ssl-server-name.
	u, err = ParseURI("mysql://root@db.example.com/mydb?tls=verify-full&ssl-server-name=other.example.com")
	if err != nil {
		t.Fatalf("ParseURI(verify-full): %v", err)
	}
	if cfg := u.TLSConfig(); cfg == nil || cfg.ServerName != "other.example.com" {
		t.Fatalf("verify-full server name = %+v", cfg)
	}

	for _, bad := range []string{
		"mysql://root@localhost/mydb?tls=bogus",
		"mysql://root@localhost/mydb?ssl-ca=/tmp/ca.pem", // ssl-* without tls
		"mysql://root@localhost/mydb?tls=verify-full&ssl-cert=/tmp/c.pem",
		"mysql://root@localhost/mydb?tls=verify-full&ssl-key=/tmp/k.pem",
	} {
		if _, err := ParseURI(bad); err == nil {
			t.Fatalf("ParseURI(%q) must fail", bad)
		}
	}
}

func TestParseURITLSCustomMaterial(t *testing.T) {
	cert, key := writeTestCert(t)
	uri := "mysql://root@localhost/mydb?tls=verify-full&ssl-ca=" + cert + "&ssl-cert=" + cert + "&ssl-key=" + key
	u, err := ParseURI(uri)
	if err != nil {
		t.Fatalf("ParseURI: %v", err)
	}
	cfg := u.TLSConfig()
	if cfg == nil || cfg.RootCAs == nil {
		t.Fatalf("RootCAs not set: %+v", cfg)
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("client certs = %d, want 1", len(cfg.Certificates))
	}

	dsn, err := u.QueryDSN()
	if err != nil {
		t.Fatalf("QueryDSN: %v", err)
	}
	if !strings.Contains(dsn, "tls=urutau-") {
		t.Fatalf("QueryDSN = %q, want a registered tls name", dsn)
	}
}

// writeTestCert generates a self-signed cert/key pair and returns their paths.
// The cert doubles as a CA for the ssl-ca test.
func writeTestCert(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "urutau-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}

	dir := t.TempDir()
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certPath, keyPath
}

func TestParseURITimezone(t *testing.T) {
	// Defaults to UTC.
	u, err := ParseURI("mysql://root@localhost/mydb")
	if err != nil {
		t.Fatalf("ParseURI: %v", err)
	}
	if u.TimeLocation() != time.UTC {
		t.Fatalf("default loc = %v, want UTC", u.TimeLocation())
	}

	u, err = ParseURI("mysql://root@localhost/mydb?timezone=America/Sao_Paulo")
	if err != nil {
		t.Fatalf("ParseURI(timezone): %v", err)
	}
	if got := u.TimeLocation().String(); got != "America/Sao_Paulo" {
		t.Fatalf("loc = %q", got)
	}
	dsn, err := u.QueryDSN()
	if err != nil {
		t.Fatalf("QueryDSN: %v", err)
	}
	// The session is pinned to UTC; the operator's zone is applied in Go, so
	// it must NOT leak into the DSN.
	if !strings.Contains(dsn, "loc=UTC") || !strings.Contains(dsn, "time_zone=") {
		t.Fatalf("QueryDSN = %q, want a UTC-pinned session", dsn)
	}
	if strings.Contains(dsn, "Sao_Paulo") {
		t.Fatalf("QueryDSN = %q, must not carry the operator zone", dsn)
	}

	if _, err := ParseURI("mysql://root@localhost/mydb?timezone=Not/AZone"); err == nil {
		t.Fatal("an unknown timezone must be rejected")
	}
}
