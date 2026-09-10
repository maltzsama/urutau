// Package grpctls builds mutual-TLS gRPC credentials for the coordinator
// (server) and worker (client) control plane. Without it the Assignment —
// which carries the source DSN — and every Shutdown/Ack ride plaintext on
// the wire (CD-1b).
//
// An empty Config means the caller chose plaintext; it is the caller's job
// to warn loudly (the coordinator does). All three fields must be set to
// enable mTLS.
package grpctls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Config is mutual-TLS material.
type Config struct {
	CertFile     string // this side's certificate
	KeyFile      string // this side's private key
	ClientCAFile string // CA that signs the peer cert (server: client CA; client: server CA)
}

// Enabled reports whether any TLS material was configured.
func (c Config) Enabled() bool {
	return c.CertFile != "" || c.KeyFile != "" || c.ClientCAFile != ""
}

func (c Config) validate() error {
	if c.CertFile == "" || c.KeyFile == "" || c.ClientCAFile == "" {
		return fmt.Errorf("grpctls: cert, key and CA must all be set to enable mTLS")
	}
	return nil
}

func (c Config) pool() (*x509.CertPool, error) {
	pem, err := os.ReadFile(c.ClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("grpctls: read CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("grpctls: CA %q has no certificates", c.ClientCAFile)
	}
	return pool, nil
}

// ServerTLS builds the server *tls.Config: present the server cert and
// REQUIRE + verify a client certificate (mutual TLS). TLS 1.3 floor.
func (c Config) ServerTLS() (*tls.Config, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("grpctls: load keypair: %w", err)
	}
	pool, err := c.pool()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// ServerOption builds the gRPC server option from ServerTLS.
func (c Config) ServerOption() (grpc.ServerOption, error) {
	cfg, err := c.ServerTLS()
	if err != nil {
		return nil, err
	}
	return grpc.Creds(credentials.NewTLS(cfg)), nil
}

// ClientTLS builds the client *tls.Config: present the client cert and
// verify the server against the CA. TLS 1.3 floor.
func (c Config) ClientTLS() (*tls.Config, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("grpctls: load keypair: %w", err)
	}
	pool, err := c.pool()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// ClientCreds builds transport credentials from ClientTLS.
func (c Config) ClientCreds() (credentials.TransportCredentials, error) {
	cfg, err := c.ClientTLS()
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(cfg), nil
}
