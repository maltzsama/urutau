package postgres

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"runtime"
	"sync"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/ssh"

	"github.com/maltzsama/urutau/spec"
)

// Defaults applied when the operator leaves a tuning field unset on the
// nested postgres block. Both mirror the OLake reference behavior.
const (
	defaultMaxThreads = 0 // resolved to runtime.NumCPU() at build time
	defaultRetryCount = 3
	maxMaxThreads     = 32
)

// ConnConfig holds the resolved connection parameters, built from either a
// URI or the nested PostgresSource config.
type ConnConfig struct {
	// QueryURI is the libpq DSN rendered from the config. It is what travels
	// to a distributed worker (the worker only receives kind + dsn).
	QueryURI string
	// ConnConfig is the native pgx config for replication and query
	// connections. It carries TLSConfig and DialFunc (SSH) so both
	// connection paths share one transport.
	ConnConfig *pgx.ConnConfig
	// MaxOpenConns is the resolved maxThreads value.
	MaxOpenConns int
	// RetryCount is the resolved number of transient-connection retries.
	RetryCount int

	tunnel *sshTunnel
}

// Close tears down the SSH tunnel, if one was established. Safe to call
// multiple times.
func (c *ConnConfig) Close() error {
	if c == nil || c.tunnel == nil {
		return nil
	}
	return c.tunnel.Close()
}

// BuildConnConfig constructs a ConnConfig from a URI string.
func BuildConnConfig(uri string) (*ConnConfig, error) {
	cfg, err := pgx.ParseConfig(uri)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse uri: %w", err)
	}
	return &ConnConfig{
		QueryURI:     uri,
		ConnConfig:   cfg,
		MaxOpenConns: resolveMaxThreads(0),
		RetryCount:   defaultRetryCount,
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

	cfg := &pgx.ConnConfig{}
	cfg.Host = pg.Host
	cfg.Port = uint16(port)
	cfg.Database = pg.Database
	cfg.User = pg.Username
	cfg.Password = pg.Password

	if len(pg.Params) > 0 {
		cfg.RuntimeParams = make(map[string]string, len(pg.Params))
		for k, v := range pg.Params {
			cfg.RuntimeParams[k] = v
		}
	}

	// TLS: a nil TLSConfig means no TLS. pgx defaults to TLS-require, so an
	// absent/disable ssl block must clear it explicitly.
	if pg.SSL != nil && pg.SSL.Mode != "" && pg.SSL.Mode != "disable" {
		tlsCfg, err := buildTLSConfig(pg.SSL)
		if err != nil {
			return nil, fmt.Errorf("postgres: ssl: %w", err)
		}
		cfg.TLSConfig = tlsCfg
	} else {
		cfg.TLSConfig = nil
	}

	cc := &ConnConfig{
		QueryURI:     pg.DSN(),
		ConnConfig:   cfg,
		MaxOpenConns: resolveMaxThreads(pg.MaxThreads),
		RetryCount:   resolveRetryCount(pg.RetryCount),
	}

	// SSH: one tunnel per ConnConfig, shared by the query and replication
	// connections and torn down by Close.
	if pg.SSH != nil {
		tunnel, err := newSSHTunnel(pg.SSH)
		if err != nil {
			return nil, fmt.Errorf("postgres: ssh: %w", err)
		}
		cc.tunnel = tunnel
		cfg.DialFunc = tunnel.Dial
	}

	return cc, nil
}

// resolveMaxThreads applies the maxThreads default (runtime.NumCPU()) and the
// 1..32 bound. The spec validator already rejects an out-of-range explicit
// value; this clamps the derived default.
func resolveMaxThreads(n int) int {
	if n == 0 {
		n = runtime.NumCPU()
	}
	if n < 1 {
		n = 1
	}
	if n > maxMaxThreads {
		n = maxMaxThreads
	}
	return n
}

// resolveRetryCount applies the retryCount default (3). A negative value is
// clamped to 0 (the spec validator rejects it, this is defense in depth).
func resolveRetryCount(n int) int {
	if n == 0 {
		return defaultRetryCount
	}
	if n < 0 {
		return 0
	}
	return n
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
		pool, err := loadCA(ssl.CA)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool
		// Go's default verification always checks the hostname; disable it
		// and re-verify the chain by hand (DNSName left empty).
		cfg.InsecureSkipVerify = true
		cfg.VerifyConnection = verifyChainOnlyPostgres(cfg.RootCAs)
	case "verify-full":
		pool, err := loadCA(ssl.CA)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool
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
	if path == "" {
		return nil, fmt.Errorf("ssl.ca is required for this mode")
	}
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

// sshTunnel owns one SSH client shared by every connection the source opens
// through it. A fresh client is dialed on first use and reused; if a forward
// fails the dead client is dropped so the next dial reconnects. Close tears
// it down at shutdown.
type sshTunnel struct {
	addr   string
	client *ssh.ClientConfig

	mu     sync.Mutex
	conn   *ssh.Client
	closed bool
}

// newSSHTunnel validates the SSH config and prepares (but does not dial) the
// tunnel. No network I/O happens until the first connection.
func newSSHTunnel(cfg *spec.SSHConfig) (*sshTunnel, error) {
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

	port := cfg.Port
	if port == 0 {
		port = 22
	}
	return &sshTunnel{
		addr: fmt.Sprintf("%s:%d", cfg.Host, port),
		client: &ssh.ClientConfig{
			User:            cfg.Username,
			Auth:            authMethods,
			HostKeyCallback: ssh.InsecureIgnoreHostKey(), //nolint:gosec // operator-managed tunnel
		},
	}, nil
}

// Dial returns a connection forwarded through the tunnel. It matches
// pgx.ConnConfig.DialFunc.
func (t *sshTunnel) Dial(_ context.Context, network, addr string) (net.Conn, error) {
	client, err := t.getClient()
	if err != nil {
		return nil, err
	}
	conn, err := client.Dial(network, addr)
	if err != nil {
		// The cached client may be dead; drop it so the next dial reconnects.
		t.drop(client)
		return nil, fmt.Errorf("ssh forward to %s: %w", addr, err)
	}
	return conn, nil
}

func (t *sshTunnel) getClient() (*ssh.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, fmt.Errorf("ssh tunnel closed")
	}
	if t.conn != nil {
		return t.conn, nil
	}
	client, err := ssh.Dial("tcp", t.addr, t.client)
	if err != nil {
		return nil, fmt.Errorf("ssh dial %s: %w", t.addr, err)
	}
	t.conn = client
	return client, nil
}

func (t *sshTunnel) drop(client *ssh.Client) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.conn == client {
		_ = t.conn.Close()
		t.conn = nil
	}
}

// Close tears down the SSH client. Safe to call multiple times.
func (t *sshTunnel) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	if t.conn == nil {
		return nil
	}
	err := t.conn.Close()
	t.conn = nil
	return err
}
