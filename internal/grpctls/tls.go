// Package grpctls builds mutual-TLS gRPC credentials for the coordinator
// (server) and worker (client) control plane. Without it the Assignment —
// which carries the source DSN — and every Shutdown/Ack ride plaintext on
// the wire (CD-1b).
//
// An empty Config means plaintext. The coordinator treats that as an error at
// boot unless AllowInsecure is set explicitly (fail closed, not silently over
// the wire); it is still the caller's job to warn. All three fields must be
// set to enable mTLS.
package grpctls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
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
	// AllowInsecure accepts a plaintext control plane. It is the explicit
	// opt-out of the fail-closed default: RequireTLS errors when no material
	// is set and this is false.
	AllowInsecure bool
}

// Enabled reports whether any TLS material was configured.
func (c Config) Enabled() bool {
	return c.CertFile != "" || c.KeyFile != "" || c.ClientCAFile != ""
}

// RequireTLS reports whether the config is a usable mTLS set, or a plaintext
// config the caller explicitly allowed. It is the fail-closed gate the
// coordinator runs at boot: with no material and no AllowInsecure, the
// control plane would send the source DSN in the clear, so boot must fail.
func (c Config) RequireTLS() error {
	if c.Enabled() || c.AllowInsecure {
		return nil
	}
	return errors.New("grpctls: control plane is plaintext — the assignment carries the source DSN; set cert, key and CA, or allow insecure explicitly")
}

func (c Config) validate() error {
	if c.CertFile == "" || c.KeyFile == "" || c.ClientCAFile == "" {
		return fmt.Errorf("grpctls: cert, key and CA must all be set to enable mTLS")
	}
	return nil
}

// Validate reports whether the config is a coherent mTLS set: either all
// three paths are set, or none are. It lets a CLI fail fast on a partial
// set at flag-parse time instead of at the first connection.
func (c Config) Validate() error {
	if !c.Enabled() {
		return nil
	}
	return c.validate()
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
