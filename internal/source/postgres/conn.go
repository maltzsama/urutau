package postgres

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/ssh"

	"github.com/maltzsama/urutau/spec"
)

// ConnConfig holds the resolved connection parameters, built from either a
// URI or the nested PostgresSource config.
type ConnConfig struct {
	// QueryURI is the DSN for sql.Open("pgx", ...).
	QueryURI string
	// ConnConfig is the native pgx config for replication connections.
	ConnConfig *pgx.ConnConfig
	// MaxOpenConns is the maxThreads value (0 means leave default).
	MaxOpenConns int
	// RetryCount is the number of transient-connection retries.
	RetryCount int
}

// BuildConnConfig constructs a ConnConfig from a URI string.
func BuildConnConfig(uri string) (*ConnConfig, error) {
	cfg, err := pgx.ParseConfig(uri)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse uri: %w", err)
	}
	return &ConnConfig{
		QueryURI:   uri,
		ConnConfig: cfg,
	}, nil
}

// BuildConnConfigFromPostgres constructs a ConnConfig from the nested
// PostgresSource config. When present, source.uri is ignored for connection
// building.
func BuildConnConfigFromPostgres(pg *spec.PostgresSource) (*ConnConfig, error) {
	if pg.Host == "" {
		return nil, fmt.Errorf("postgres: host required")
	}
	if pg.Database == "" {
		return nil, fmt.Errorf("postgres: database required")
	}
	port := pg.Port
	if port == 0 {
		port = 5432
	}

	// Build native pgx ConnConfig.
	cfg := &pgx.ConnConfig{}
	cfg.Host = pg.Host
	cfg.Port = uint16(port)
	cfg.Database = pg.Database
	if pg.Username != "" {
		cfg.User = pg.Username
	}
	if pg.Password != "" {
		cfg.Password = pg.Password
	}

	// Pass through any extra params.
	if len(pg.Params) > 0 {
		cfg.RuntimeParams = make(map[string]string, len(pg.Params))
		for k, v := range pg.Params {
			cfg.RuntimeParams[k] = v
		}
	}

	// Apply TLS.
	if pg.SSL != nil && pg.SSL.Mode != "" && pg.SSL.Mode != "disable" {
		tlsCfg, err := buildTLSConfig(pg.SSL)
		if err != nil {
			return nil, fmt.Errorf("postgres: ssl: %w", err)
		}
		cfg.TLSConfig = tlsCfg
	} else {
		// pgx defaults to TLS require; disable when no SSL block.
		cfg.TLSConfig = nil
	}

	// Apply SSH tunnel.
	if pg.SSH != nil {
		dialer, err := sshDialer(pg.SSH)
		if err != nil {
			return nil, fmt.Errorf("postgres: ssh: %w", err)
		}
		cfg.DialFunc = dialer
	}

	// Build query DSN for sql.Open.
	queryURI := buildDSN(pg)

	cc := &ConnConfig{
		QueryURI:   queryURI,
		ConnConfig: cfg,
	}
	if pg.MaxThreads > 0 {
		cc.MaxOpenConns = pg.MaxThreads
	}
	if pg.RetryCount > 0 {
		cc.RetryCount = pg.RetryCount
	}
	return cc, nil
}

// buildDSN constructs a pgx connection string (DSN) from the nested config.
func buildDSN(pg *spec.PostgresSource) string {
	port := pg.Port
	if port == 0 {
		port = 5432
	}
	var b strings.Builder
	fmt.Fprintf(&b, "host=%s port=%d dbname=%s", pg.Host, port, pg.Database)
	if pg.Username != "" {
		fmt.Fprintf(&b, " user=%s", pg.Username)
	}
	if pg.Password != "" {
		fmt.Fprintf(&b, " password=%s", pg.Password)
	}
	// SSL mode
	sslMode := "disable"
	if pg.SSL != nil && pg.SSL.Mode != "" {
		sslMode = pg.SSL.Mode
	}
	fmt.Fprintf(&b, " sslmode=%s", sslMode)
	if pg.SSL != nil {
		if pg.SSL.CA != "" {
			fmt.Fprintf(&b, " sslrootcert=%s", pg.SSL.CA)
		}
		if pg.SSL.Cert != "" {
			fmt.Fprintf(&b, " sslcert=%s", pg.SSL.Cert)
		}
		if pg.SSL.Key != "" {
			fmt.Fprintf(&b, " sslkey=%s", pg.SSL.Key)
		}
	}
	for k, v := range pg.Params {
		fmt.Fprintf(&b, " %s=%s", k, v)
	}
	return b.String()
}

// buildTLSConfig builds a *tls.Config from the SSL config. Returns nil for
// disable mode.
func buildTLSConfig(ssl *spec.SSLConfig) (*tls.Config, error) {
	if ssl.Mode == "" || ssl.Mode == "disable" {
		return nil, nil
	}

	cfg := &tls.Config{MinVersion: tls.VersionTLS12}

	switch ssl.Mode {
	case "require":
		// TLS but don't verify the server certificate.
		cfg.InsecureSkipVerify = true
	case "verify-ca":
		// TLS, verify the chain against roots but not hostname.
		if ssl.CA != "" {
			pool, err := loadCA(ssl.CA)
			if err != nil {
				return nil, err
			}
			cfg.RootCAs = pool
		}
		// InsecureSkipVerify + VerifyConnection for chain-only check.
		cfg.InsecureSkipVerify = true
		cfg.VerifyConnection = verifyChainOnlyPostgres(cfg.RootCAs)
	case "verify-full":
		// TLS, verify chain and hostname.
		if ssl.CA != "" {
			pool, err := loadCA(ssl.CA)
			if err != nil {
				return nil, err
			}
			cfg.RootCAs = pool
		}
	}

	// Client certificate (mutual TLS).
	if ssl.Cert != "" || ssl.Key != "" {
		if ssl.Cert == "" || ssl.Key == "" {
			return nil, fmt.Errorf("ssl.cert and ssl.key must be set together")
		}
		cert, err := tls.LoadX509KeyPair(ssl.Cert, ssl.Key)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	return cfg, nil
}

func loadCA(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read ssl.ca %q: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("ssl.ca %q: no certificates found", path)
	}
	return pool, nil
}

// verifyChainOnlyPostgres returns a tls.ConnectionState verifier that checks
// the peer certificate chain against roots but not the hostname.
func verifyChainOnlyPostgres(roots *x509.CertPool) func(tls.ConnectionState) error {
	return func(cs tls.ConnectionState) error {
		if len(cs.PeerCertificates) == 0 {
			return fmt.Errorf("server presented no certificate")
		}
		opts := x509.VerifyOptions{
			Roots:         roots,
			Intermediates: x509.NewCertPool(),
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}
		for _, cert := range cs.PeerCertificates[1:] {
			opts.Intermediates.AddCert(cert)
		}
		_, err := cs.PeerCertificates[0].Verify(opts)
		return err
	}
}

// sshDialer creates a DialFunc that tunnels through an SSH server.
func sshDialer(cfg *spec.SSHConfig) (func(ctx context.Context, network, addr string) (net.Conn, error), error) {
	var authMethods []ssh.AuthMethod
	if cfg.Password != "" {
		authMethods = append(authMethods, ssh.Password(cfg.Password))
	}
	if cfg.PrivateKey != "" {
		key, err := os.ReadFile(cfg.PrivateKey)
		if err != nil {
			return nil, fmt.Errorf("read private key %q: %w", cfg.PrivateKey, err)
		}
		var signer ssh.Signer
		if cfg.Passphrase != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase(key, []byte(cfg.Passphrase))
		} else {
			signer, err = ssh.ParsePrivateKey(key)
		}
		if err != nil {
			return nil, fmt.Errorf("parse private key: %w", err)
		}
		authMethods = append(authMethods, ssh.PublicKeys(signer))
	}
	if len(authMethods) == 0 {
		return nil, fmt.Errorf("no authentication method configured")
	}

	sshPort := cfg.Port
	if sshPort == 0 {
		sshPort = 22
	}
	sshAddr := fmt.Sprintf("%s:%d", cfg.Host, sshPort)

	sshCfg := &ssh.ClientConfig{
		User:            cfg.Username,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), //nolint:gosec // operator-managed tunnel
	}

	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		client, err := ssh.Dial("tcp", sshAddr, sshCfg)
		if err != nil {
			return nil, fmt.Errorf("ssh dial %s: %w", sshAddr, err)
		}
		conn, err := client.Dial(network, addr)
		if err != nil {
			return nil, fmt.Errorf("ssh forward to %s: %w", addr, err)
		}
		return conn, nil
	}, nil
}
