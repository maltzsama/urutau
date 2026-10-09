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

	"github.com/maltzsama/urutau/internal/source/kafka/decoder"
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
	return tlsConfigFromFiles(t.CA, t.Cert, t.Key, t.InsecureSkipVerify)
}

// tlsConfigFromFiles builds a *tls.Config from PEM file paths: a CA pool for
// server verification and, when cert and key are both set, a client
// certificate for mutual TLS.
func tlsConfigFromFiles(ca, cert, key string, insecure bool) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: insecure} //nolint:gosec // opt-in via the spec
	if ca != "" {
		pemBytes, err := os.ReadFile(ca)
		if err != nil {
			return nil, fmt.Errorf("kafka: tls ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("kafka: tls ca %s: no certificates parsed", ca)
		}
		cfg.RootCAs = pool
	}
	if cert != "" && key != "" {
		pair, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("kafka: tls client cert: %w", err)
		}
		cfg.Certificates = []tls.Certificate{pair}
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

// registryFor builds the schema-registry client from the source spec,
// applying the optional basic auth and TLS (issue #604).
func registryFor(src spec.Source) (*decoder.HTTPRegistry, error) {
	a := src.SchemaRegistryAuth
	if a == nil {
		return decoder.NewHTTPRegistry(src.SchemaRegistry), nil
	}
	// Only install a custom transport when there is TLS material, so plain
	// basic-auth keeps the default transport (proxy env, pooling).
	var tlsCfg *tls.Config
	if a.CA != "" || a.Cert != "" || a.InsecureSkipVerify {
		cfg, err := tlsConfigFromFiles(a.CA, a.Cert, a.Key, a.InsecureSkipVerify)
		if err != nil {
			return nil, err
		}
		tlsCfg = cfg
	}
	return decoder.NewHTTPRegistryWithAuth(src.SchemaRegistry, a.Username, a.Password, tlsCfg), nil
}
