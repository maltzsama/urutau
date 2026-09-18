package postgres

import (
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/maltzsama/urutau/spec"
)

func TestHostKeyCallback(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := ssh.NewPublicKey(priv.Public())
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	kh := filepath.Join(dir, "known_hosts")
	line := knownhosts.Line([]string{"bastion.example.com"}, pub)
	if err := os.WriteFile(kh, []byte(line+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := hostKeyCallback(&spec.SSHConfig{KnownHosts: kh}); err != nil {
		t.Fatalf("hostKeyCallback: %v", err)
	}
	if _, err := hostKeyCallback(&spec.SSHConfig{KnownHosts: filepath.Join(dir, "missing")}); err == nil {
		t.Fatal("want error for a missing known_hosts file")
	}
}

func TestBuildConnConfigFromPostgresMinimal(t *testing.T) {
	pg := &spec.PostgresSource{Host: "localhost", Database: "mydb"}
	cc, err := BuildConnConfigFromPostgres(pg)
	if err != nil {
		t.Fatal(err)
	}
	if cc.QueryURI == "" {
		t.Fatal("QueryURI must not be empty")
	}
	if cc.ConnConfig == nil {
		t.Fatal("ConnConfig must not be nil")
	}
	if cc.ConnConfig.Host != "localhost" {
		t.Fatalf("host = %q, want localhost", cc.ConnConfig.Host)
	}
	if cc.ConnConfig.Database != "mydb" {
		t.Fatalf("database = %q, want mydb", cc.ConnConfig.Database)
	}
	if cc.ConnConfig.Port != 5432 {
		t.Fatalf("port = %d, want 5432", cc.ConnConfig.Port)
	}
}

func TestBuildConnConfigFromPostgresWithPort(t *testing.T) {
	pg := &spec.PostgresSource{Host: "db.local", Database: "test", Port: 5433}
	cc, err := BuildConnConfigFromPostgres(pg)
	if err != nil {
		t.Fatal(err)
	}
	if cc.ConnConfig.Port != 5433 {
		t.Fatalf("port = %d, want 5433", cc.ConnConfig.Port)
	}
}

func TestBuildConnConfigFromPostgresWithCredentials(t *testing.T) {
	pg := &spec.PostgresSource{
		Host:     "db.local",
		Database: "test",
		Username: "admin",
		Password: "secret",
	}
	cc, err := BuildConnConfigFromPostgres(pg)
	if err != nil {
		t.Fatal(err)
	}
	if cc.ConnConfig.User != "admin" {
		t.Fatalf("user = %q, want admin", cc.ConnConfig.User)
	}
	if cc.ConnConfig.Password != "secret" {
		t.Fatalf("password = %q, want secret", cc.ConnConfig.Password)
	}
}

func TestBuildConnConfigFromPostgresWithParams(t *testing.T) {
	pg := &spec.PostgresSource{
		Host:     "db.local",
		Database: "test",
		Params:   map[string]string{"application_name": "urutau"},
	}
	cc, err := BuildConnConfigFromPostgres(pg)
	if err != nil {
		t.Fatal(err)
	}
	if cc.ConnConfig.RuntimeParams["application_name"] != "urutau" {
		t.Fatalf("param application_name = %q, want urutau", cc.ConnConfig.RuntimeParams["application_name"])
	}
}

func TestBuildConnConfigFromPostgresMaxThreadsAndRetry(t *testing.T) {
	pg := &spec.PostgresSource{
		Host:       "db.local",
		Database:   "test",
		MaxThreads: 8,
		RetryCount: 3,
	}
	cc, err := BuildConnConfigFromPostgres(pg)
	if err != nil {
		t.Fatal(err)
	}
	if cc.MaxOpenConns != 8 {
		t.Fatalf("MaxOpenConns = %d, want 8", cc.MaxOpenConns)
	}
	if cc.RetryCount != 3 {
		t.Fatalf("RetryCount = %d, want 3", cc.RetryCount)
	}
}

func TestBuildConnConfigFromPostgresRejectsMissingHost(t *testing.T) {
	pg := &spec.PostgresSource{Database: "mydb"}
	if _, err := BuildConnConfigFromPostgres(pg); err == nil {
		t.Fatal("want error for missing host")
	}
}

func TestBuildConnConfigFromPostgresRejectsMissingDatabase(t *testing.T) {
	pg := &spec.PostgresSource{Host: "localhost"}
	if _, err := BuildConnConfigFromPostgres(pg); err == nil {
		t.Fatal("want error for missing database")
	}
}

func TestBuildDSN(t *testing.T) {
	pg := &spec.PostgresSource{
		Host:     "db.local",
		Port:     5433,
		Database: "mydb",
		Username: "admin",
		Password: "s3cret",
	}
	dsn := pg.DSN()
	// Check essential parts are present.
	for _, want := range []string{"host=db.local", "port=5433", "dbname=mydb", "user=admin", "password=s3cret", "sslmode=disable"} {
		if !contains(dsn, want) {
			t.Errorf("DSN %q missing %q", dsn, want)
		}
	}
}

func TestBuildDSNDefaultPort(t *testing.T) {
	pg := &spec.PostgresSource{Host: "localhost", Database: "db"}
	dsn := pg.DSN()
	if !contains(dsn, "port=5432") {
		t.Errorf("DSN %q missing default port 5432", dsn)
	}
}

func TestBuildDSNWithSSL(t *testing.T) {
	pg := &spec.PostgresSource{
		Host:     "localhost",
		Database: "db",
		SSL:      &spec.SSLConfig{Mode: "verify-full", CA: "/ca.pem", Cert: "/cert.pem", Key: "/key.pem"},
	}
	dsn := pg.DSN()
	for _, want := range []string{"sslmode=verify-full", "sslrootcert=/ca.pem", "sslcert=/cert.pem", "sslkey=/key.pem"} {
		if !contains(dsn, want) {
			t.Errorf("DSN %q missing %q", dsn, want)
		}
	}
}

func TestBuildDSNWithParams(t *testing.T) {
	pg := &spec.PostgresSource{
		Host:     "localhost",
		Database: "db",
		Params:   map[string]string{"application_name": "urutau"},
	}
	dsn := pg.DSN()
	if !contains(dsn, "application_name=urutau") {
		t.Errorf("DSN %q missing param", dsn)
	}
}

func TestDSNQuotesValues(t *testing.T) {
	pg := &spec.PostgresSource{Host: "localhost", Database: "db", Password: "p a'ss"}
	dsn := pg.DSN()
	if !contains(dsn, `password='p a\'ss'`) {
		t.Errorf("DSN %q did not quote the password", dsn)
	}
	// The rendered DSN must round-trip through pgx's parser.
	if _, err := pgx.ParseConfig(dsn); err != nil {
		t.Fatalf("rendered DSN does not parse: %v", err)
	}
}

func TestBuildConnConfigDefaults(t *testing.T) {
	pg := &spec.PostgresSource{Host: "localhost", Database: "mydb"}
	cc, err := BuildConnConfigFromPostgres(pg)
	if err != nil {
		t.Fatal(err)
	}
	if cc.MaxOpenConns < 1 || cc.MaxOpenConns > 32 {
		t.Fatalf("default MaxOpenConns = %d, want 1..32", cc.MaxOpenConns)
	}
	if cc.RetryCount != 3 {
		t.Fatalf("default RetryCount = %d, want 3", cc.RetryCount)
	}
}

func TestBuildConnConfigFromPostgresTLSModes(t *testing.T) {
	// disable -> no TLS
	cc, err := BuildConnConfigFromPostgres(&spec.PostgresSource{
		Host: "localhost", Database: "db", SSL: &spec.SSLConfig{Mode: "disable"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if cc.ConnConfig.TLSConfig != nil {
		t.Fatal("disable mode must not configure TLS")
	}

	// require -> TLS without verification
	cc, err = BuildConnConfigFromPostgres(&spec.PostgresSource{
		Host: "localhost", Database: "db", SSL: &spec.SSLConfig{Mode: "require"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if cc.ConnConfig.TLSConfig == nil {
		t.Fatal("require mode must configure TLS")
	}
	if !cc.ConnConfig.TLSConfig.InsecureSkipVerify {
		t.Fatal("require mode should skip verification")
	}
}

func TestBuildConnConfigFromPostgresCertWithoutKey(t *testing.T) {
	_, err := BuildConnConfigFromPostgres(&spec.PostgresSource{
		Host: "localhost", Database: "db", SSL: &spec.SSLConfig{Mode: "require", Cert: "/cert.pem"},
	})
	if err == nil {
		t.Fatal("want error for cert without key")
	}
}

func TestBuildConnConfigFromPostgresRuntimeParamsInitialized(t *testing.T) {
	// Reader.New writes "replication" into RuntimeParams; a nil map panics.
	cc, err := BuildConnConfigFromPostgres(&spec.PostgresSource{Host: "localhost", Database: "db"})
	if err != nil {
		t.Fatal(err)
	}
	if cc.ConnConfig.RuntimeParams == nil {
		t.Fatal("RuntimeParams must be initialized")
	}
	// The config must be one pgx created, or ConnectConfig panics.
	if _, err := pgx.ParseConfig(cc.QueryURI); err != nil {
		t.Fatalf("QueryURI must parse: %v", err)
	}
	cc.ConnConfig.Copy() // must not panic
}

func TestBuildConnConfigFromURIFallback(t *testing.T) {
	cc, err := BuildConnConfig("postgres://localhost:5432/mydb?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	if cc.ConnConfig.Host != "localhost" {
		t.Fatalf("host = %q, want localhost", cc.ConnConfig.Host)
	}
	if cc.ConnConfig.Database != "mydb" {
		t.Fatalf("database = %q, want mydb", cc.ConnConfig.Database)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstr(s, substr))
}

func containsSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
