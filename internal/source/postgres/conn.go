package postgres

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/maltzsama/urutau/spec"
)

// Defaults applied when the operator leaves a tuning field unset on the
// nested postgres block. Both mirror the OLake reference behavior.
const (
	defaultRetryCount = 3
	maxMaxThreads     = 32
	// defaultInitialWait and minInitialWait bound the CDC initial WAL wait.
	defaultInitialWait = 300 * time.Second
	minInitialWait     = 30 * time.Second
)

// sshDialTimeout bounds the TCP dial to the bastion and the SSH handshake.
const sshDialTimeout = 15 * time.Second

// ConnConfig holds the resolved connection parameters, built from either a
// URI or the nested PostgresSource config.
type ConnConfig struct {
	// QueryURI is the libpq DSN rendered from the config. It is what travels
	// to a distributed worker (the worker only receives kind + dsn).
	QueryURI string
	// ConnConfig is the native pgx config for replication and query
	// connections. It carries TLSConfig (built by pgx from the DSN) and
	// DialFunc (SSH) so both connection paths share one transport.
	ConnConfig *pgx.ConnConfig
	// MaxOpenConns is the resolved maxThreads value.
	MaxOpenConns int
	// RetryCount is the resolved number of transient-connection retries.
	RetryCount int
	// InitialWaitTime is the resolved CDC initial-WAL-message wait.
	InitialWaitTime time.Duration
	// Plugin is the resolved logical decoding plugin ("pgoutput"|"wal2json").
	Plugin string

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
		QueryURI:        uri,
		ConnConfig:      cfg,
		MaxOpenConns:    resolveMaxThreads(0),
		RetryCount:      defaultRetryCount,
		InitialWaitTime: defaultInitialWait,
		Plugin:          "pgoutput",
	}, nil
}

// BuildConnConfigFromPostgres constructs a ConnConfig from the nested
// PostgresSource config. When present, source.uri is ignored for connection
// building.
//
// The config is created by pgx.ParseConfig on the rendered DSN — never by
// hand. pgx's ConnectConfig panics on a config it did not create, and it also
// needs RuntimeParams initialized (Reader.New writes "replication" into it).
// Parsing the DSN also gets pgx's native TLS handling for free (require,
// verify-ca and verify-full all follow libpq semantics, including loading the
// CA and client cert/key). Only the SSH DialFunc is layered on afterwards.
func BuildConnConfigFromPostgres(pg *spec.PostgresSource) (*ConnConfig, error) {
	if pg.Host == "" {
		return nil, fmt.Errorf("postgres: host required")
	}
	if pg.Database == "" {
		return nil, fmt.Errorf("postgres: database required")
	}

	dsn := pg.DSN()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse config: %w", err)
	}

	cc := &ConnConfig{
		QueryURI:        dsn,
		ConnConfig:      cfg,
		MaxOpenConns:    resolveMaxThreads(pg.MaxThreads),
		RetryCount:      resolveRetryCount(pg.RetryCount),
		InitialWaitTime: resolveInitialWaitTime(cdcInitialWait(pg)),
		Plugin:          resolvePlugin(pg),
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
		// pgx resolves the host to IPs BEFORE calling DialFunc, which would
		// require the database hostname to resolve locally — defeating the
		// tunnel for a host only reachable through the bastion. Return the
		// host unchanged so DialFunc receives it verbatim and the bastion
		// does the resolution.
		cfg.LookupFunc = func(_ context.Context, host string) ([]string, error) {
			return []string{host}, nil
		}
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

// resolveInitialWaitTime applies the initialWaitTime default (300s). A
// positive value below the minimum is clamped up (the spec validator rejects
// it, this is defense in depth).
func resolveInitialWaitTime(seconds int) time.Duration {
	if seconds == 0 {
		return defaultInitialWait
	}
	d := time.Duration(seconds) * time.Second
	if d < minInitialWait {
		return minInitialWait
	}
	return d
}

// cdcInitialWait reads the initial-wait seconds from the cdc block (0 when
// unset, which resolveInitialWaitTime turns into the default).
func cdcInitialWait(pg *spec.PostgresSource) int {
	if pg.CDC == nil {
		return 0
	}
	return pg.CDC.InitialWaitTime
}

// resolvePlugin returns the logical decoding plugin, defaulting to pgoutput.
func resolvePlugin(pg *spec.PostgresSource) string {
	if pg.CDC != nil && pg.CDC.Plugin != "" {
		return pg.CDC.Plugin
	}
	return "pgoutput"
}

// sshTunnel owns one SSH client shared by every connection the source opens
// through it. A fresh client is dialed on first use and reused; if a forward
// fails the dead client is dropped so the next dial reconnects. Close tears
// it down at shutdown.
type sshTunnel struct {
	addr      string
	clientCfg *ssh.ClientConfig
	dialer    net.Dialer

	mu     sync.Mutex
	conn   *ssh.Client
	closed bool
}

// newSSHTunnel validates the SSH config and prepares (but does not dial) the
// tunnel. No network I/O happens until the first connection.
func newSSHTunnel(cfg *spec.SSHConfig) (*sshTunnel, error) {
	authMethods, err := sshAuthMethods(cfg)
	if err != nil {
		return nil, err
	}
	hostKey, err := hostKeyCallback(cfg)
	if err != nil {
		return nil, err
	}

	port := cfg.Port
	if port == 0 {
		port = 22
	}
	return &sshTunnel{
		addr: net.JoinHostPort(cfg.Host, strconv.Itoa(port)),
		clientCfg: &ssh.ClientConfig{
			User:            cfg.Username,
			Auth:            authMethods,
			HostKeyCallback: hostKey,
			Timeout:         sshDialTimeout,
		},
		dialer: net.Dialer{Timeout: sshDialTimeout},
	}, nil
}

// sshAuthMethods builds the authentication methods from the config: password
// and/or private key (with an optional passphrase).
func sshAuthMethods(cfg *spec.SSHConfig) ([]ssh.AuthMethod, error) {
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
	return authMethods, nil
}

// hostKeyCallback verifies the bastion's host key against a known_hosts file.
// The configured path wins; otherwise ~/.ssh/known_hosts is used when it
// exists. A missing file is an error — host key verification is never
// silently disabled.
func hostKeyCallback(cfg *spec.SSHConfig) (ssh.HostKeyCallback, error) {
	path := cfg.KnownHosts
	if path == "" {
		if home, err := os.UserHomeDir(); err == nil {
			def := filepath.Join(home, ".ssh", "known_hosts")
			if _, statErr := os.Stat(def); statErr == nil {
				path = def
			}
		}
	}
	if path == "" {
		return nil, fmt.Errorf("ssh.knownHosts is required (or provide a readable ~/.ssh/known_hosts); refusing to disable host key verification")
	}
	cb, err := knownhosts.New(path)
	if err != nil {
		return nil, fmt.Errorf("load known_hosts %q: %w", path, err)
	}
	return cb, nil
}

// Dial returns a connection forwarded through the tunnel. It matches
// pgx.ConnConfig.DialFunc and honors ctx for both the bastion dial and the
// forwarded connection.
func (t *sshTunnel) Dial(ctx context.Context, network, addr string) (net.Conn, error) {
	client, err := t.getClient(ctx)
	if err != nil {
		return nil, err
	}
	conn, err := client.DialContext(ctx, network, addr)
	if err != nil {
		// A channel-open rejection (the target refused the TCP dial) means
		// the SSH transport is still healthy: do NOT touch the shared
		// client, or one refused forward would kill the query pool and the
		// replication connection with it. Only a transport-level failure
		// marks the client dead — safe to drop, since its other channels
		// are dead too.
		if transportDead(err) {
			t.drop(client)
		}
		return nil, fmt.Errorf("ssh forward to %s: %w", addr, err)
	}
	return conn, nil
}

// transportDead reports whether a failed forward means the SSH client's
// transport is dead, as opposed to the remote target refusing the dial
// (an *ssh.OpenChannelError, which leaves the client usable for other
// channels).
func transportDead(err error) bool {
	var openErr *ssh.OpenChannelError
	return !errors.As(err, &openErr)
}

func (t *sshTunnel) getClient(ctx context.Context) (*ssh.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, fmt.Errorf("ssh tunnel closed")
	}
	if t.conn != nil {
		return t.conn, nil
	}

	netConn, err := t.dialer.DialContext(ctx, "tcp", t.addr)
	if err != nil {
		return nil, fmt.Errorf("ssh dial %s: %w", t.addr, err)
	}
	conn, chans, reqs, err := ssh.NewClientConn(netConn, t.addr, t.clientCfg)
	if err != nil {
		_ = netConn.Close()
		return nil, fmt.Errorf("ssh handshake %s: %w", t.addr, err)
	}
	t.conn = ssh.NewClient(conn, chans, reqs)
	return t.conn, nil
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
