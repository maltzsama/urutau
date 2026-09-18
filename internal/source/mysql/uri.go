package mysql

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	// Embed the IANA timezone database so `timezone=<IANA>` resolves even in a
	// scratch container without OS tzdata.
	_ "time/tzdata"

	"github.com/go-sql-driver/mysql"
)

// Accepted values of the `tls` URI parameter:
//
//	false (default) — no TLS
//	true            — TLS, verify the server against the system roots and its hostname
//	verify-full     — like true, but a custom CA (ssl-ca) replaces the system roots
//	verify-ca       — TLS, verify the chain but NOT the hostname
//	skip-verify     — TLS with no certificate verification
//
// `ssl-ca`, `ssl-cert`/`ssl-key` and `ssl-server-name` supply the CA bundle,
// the client certificate (mutual TLS) and the expected server name. They
// require a TLS mode.
const (
	tlsFalse      = "false"
	tlsTrue       = "true"
	tlsSkipVerify = "skip-verify"
	tlsVerifyCA   = "verify-ca"
	tlsVerifyFull = "verify-full"
)

// URI is a parsed MySQL source URI.
type URI struct {
	User, Password, Host, Port, DB string

	// TLS material, from the URI query parameters. tlsConfig is built once at
	// parse time (validating the mode and any cert files) and reused for both
	// the query DSN and the replication connection.
	TLSMode       string
	SSLCA         string
	SSLCert       string
	SSLKey        string
	SSLServerName string
	tlsConfig     *tls.Config
	tlsName       string // registered name for the DSN `tls=` parameter

	// Timezone is the operator-chosen IANA location for temporal columns,
	// applied to both the snapshot query and the CDC decode so a row's
	// DATETIME/TIMESTAMP value is identical whichever path reads it. Defaults
	// to UTC.
	Timezone string
	loc      *time.Location
}

// ParseURI parses a "mysql://user:pass@host:port/db" source URI, plus the
// optional TLS parameters (`tls`, `ssl-ca`, `ssl-cert`, `ssl-key`,
// `ssl-server-name`).
func ParseURI(uri string) (*URI, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("mysql: source uri: %w", err)
	}
	if u.Scheme != "mysql" {
		return nil, fmt.Errorf("mysql: source uri scheme %q, want mysql", u.Scheme)
	}
	c := &URI{User: u.User.Username()}
	if p, ok := u.User.Password(); ok {
		c.Password = p
	}
	c.Host = u.Hostname()
	if c.Host == "" {
		return nil, fmt.Errorf("mysql: source uri %q lacks host", uri)
	}
	if p := u.Port(); p != "" {
		c.Port = p
	} else {
		c.Port = "3306"
	}
	c.DB = strings.TrimPrefix(u.Path, "/")
	if c.DB == "" {
		return nil, fmt.Errorf("mysql: source uri %q lacks /db", uri)
	}

	q := u.Query()
	c.TLSMode = strings.ToLower(q.Get("tls"))
	c.SSLCA = q.Get("ssl-ca")
	c.SSLCert = q.Get("ssl-cert")
	c.SSLKey = q.Get("ssl-key")
	c.SSLServerName = q.Get("ssl-server-name")

	c.Timezone = q.Get("timezone")
	if c.Timezone == "" {
		c.Timezone = "UTC"
	}
	loc, err := time.LoadLocation(c.Timezone)
	if err != nil {
		return nil, fmt.Errorf("mysql: timezone %q: %w", c.Timezone, err)
	}
	c.loc = loc

	if c.tlsConfig, err = c.buildTLSConfig(); err != nil {
		return nil, err
	}
	if c.tlsConfig != nil {
		c.tlsName = tlsConfigName(c)
	}
	return c, nil
}

// buildTLSConfig validates the TLS parameters and builds the connection's
// *tls.Config. It returns nil when TLS is off.
func (c *URI) buildTLSConfig() (*tls.Config, error) {
	mode := c.TLSMode
	if mode == "" || mode == tlsFalse {
		if c.SSLCA != "" || c.SSLCert != "" || c.SSLKey != "" || c.SSLServerName != "" {
			return nil, fmt.Errorf("mysql: ssl-* parameters require tls=true/verify-ca/verify-full/skip-verify")
		}
		return nil, nil
	}
	switch mode {
	case tlsTrue, tlsSkipVerify, tlsVerifyCA, tlsVerifyFull:
	default:
		return nil, fmt.Errorf("mysql: unknown tls mode %q", c.TLSMode)
	}

	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if c.SSLCA != "" {
		pem, err := os.ReadFile(c.SSLCA)
		if err != nil {
			return nil, fmt.Errorf("mysql: read ssl-ca %q: %w", c.SSLCA, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("mysql: ssl-ca %q: no certificates found", c.SSLCA)
		}
		cfg.RootCAs = pool
	}
	if c.SSLCert != "" || c.SSLKey != "" {
		if c.SSLCert == "" || c.SSLKey == "" {
			return nil, fmt.Errorf("mysql: ssl-cert and ssl-key must be set together")
		}
		cert, err := tls.LoadX509KeyPair(c.SSLCert, c.SSLKey)
		if err != nil {
			return nil, fmt.Errorf("mysql: load client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	serverName := c.SSLServerName
	if serverName == "" {
		serverName = c.Host
	}
	switch mode {
	case tlsTrue, tlsVerifyFull:
		cfg.ServerName = serverName
	case tlsVerifyCA:
		// Verify the presented chain against the roots but not the hostname:
		// Go's default verification always checks the hostname, so disable it
		// and re-verify the chain by hand (DNSName left empty).
		cfg.ServerName = serverName
		cfg.InsecureSkipVerify = true
		cfg.VerifyConnection = verifyChainOnly(cfg.RootCAs)
	case tlsSkipVerify:
		cfg.InsecureSkipVerify = true
	}
	return cfg, nil
}

// verifyChainOnly returns a tls.Config.VerifyConnection that verifies the
// peer's certificate chain against roots (nil = system roots) but does not
// check the hostname — the "verify-ca" mode.
func verifyChainOnly(roots *x509.CertPool) func(tls.ConnectionState) error {
	return func(cs tls.ConnectionState) error {
		if len(cs.PeerCertificates) == 0 {
			return fmt.Errorf("mysql: tls: server presented no certificate")
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

// tlsConfigName derives a deterministic, non-reserved name under which the
// config is registered for the DSN. Deterministic so a repeated ParseURI for
// the same parameters overwrites the same entry instead of leaking one per
// call.
func tlsConfigName(c *URI) string {
	h := sha256.Sum256([]byte(strings.Join([]string{
		c.TLSMode, c.SSLCA, c.SSLCert, c.SSLKey, c.SSLServerName, c.Host,
	}, "\x00")))
	return "urutau-" + hex.EncodeToString(h[:8])
}

// TLSConfig returns the TLS configuration for the replication connection, or
// nil when TLS is off.
func (c *URI) TLSConfig() *tls.Config { return c.tlsConfig }

// TimeLocation returns the resolved temporal location (never nil; defaults to
// UTC).
func (c *URI) TimeLocation() *time.Location {
	if c.loc == nil {
		return time.UTC
	}
	return c.loc
}

// Addr returns host:port.
func (c *URI) Addr() string { return c.Host + ":" + c.Port }

// QueryDSN renders the go-sql-driver DSN for the query connection. When TLS
// is configured it registers the *tls.Config with the driver and references
// it by name.
//
// The session is pinned to UTC and the driver parses in UTC; the operator's
// timezone is applied in Go, per column type (see normalizeSnapshot and
// normalizeCol). That keeps TIMESTAMP's instant exact and interprets DATETIME
// in the operator's zone without depending on the server's timezone tables or
// freezing a DST offset (issue #139).
func (c *URI) QueryDSN() (string, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&loc=UTC&time_zone=%s",
		c.User, c.Password, c.Addr(), c.DB,
		url.QueryEscape("'+00:00'"))
	if c.tlsConfig != nil {
		if err := mysql.RegisterTLSConfig(c.tlsName, c.tlsConfig); err != nil {
			return "", fmt.Errorf("mysql: register tls config: %w", err)
		}
		dsn += "&tls=" + url.QueryEscape(c.tlsName)
	}
	return dsn, nil
}
