package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/maltzsama/urutau/spec"
)

// securityOpts builds the kgo TLS/SASL options from the source spec (issue
// #598). A nil Kafka block, or nil TLS/SASL within it, means plaintext and
// unauthenticated.
func securityOpts(src spec.Source) ([]kgo.Opt, error) {
	if src.Kafka == nil {
		return nil, nil
	}
	var opts []kgo.Opt
	if t := src.Kafka.TLS; t != nil {
		cfg, err := tlsConfig(t)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(cfg))
	}
	if s := src.Kafka.SASL; s != nil {
		mech, err := saslMechanism(s)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mech))
	}
	return opts, nil
}

// tlsConfig reads the PEM files the spec names into a *tls.Config: a CA pool
// for server verification, and — when cert and key are both set — a client
// certificate for mutual TLS.
func tlsConfig(t *spec.KafkaTLS) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: t.InsecureSkipVerify} //nolint:gosec // opt-in via the spec
	if t.CA != "" {
		pemBytes, err := os.ReadFile(t.CA)
		if err != nil {
			return nil, fmt.Errorf("kafka: tls ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("kafka: tls ca %s: no certificates parsed", t.CA)
		}
		cfg.RootCAs = pool
	}
	if t.Cert != "" && t.Key != "" {
		cert, err := tls.LoadX509KeyPair(t.Cert, t.Key)
		if err != nil {
			return nil, fmt.Errorf("kafka: tls client cert: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// saslMechanism builds the SASL mechanism the spec names. An empty mechanism
// defaults to PLAIN.
func saslMechanism(s *spec.KafkaSASL) (sasl.Mechanism, error) {
	switch s.Mechanism {
	case "", "plain":
		return plain.Auth{User: s.Username, Pass: s.Password}.AsMechanism(), nil
	case "scram-sha-256":
		return (scram.Auth{User: s.Username, Pass: s.Password}).AsSha256Mechanism(), nil
	case "scram-sha-512":
		return (scram.Auth{User: s.Username, Pass: s.Password}).AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("kafka: sasl mechanism %q is not supported", s.Mechanism)
	}
}
