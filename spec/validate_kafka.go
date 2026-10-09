package spec

import "fmt"

// validateKafkaSource validates the kafka-specific source fields: the decoder
// format, the schema registry a format needs, and the decode-error policy.
// Format and policy are rejected when used with a non-kafka kind, so the
// checks are not a no-op for other sources.
func validateKafkaSource(src Source, problems *[]string) {
	// raw is a message-log landing mode (kafka only) with no upsert semantics
	// — every message is an insert; avro needs a schema registry.
	switch src.Format {
	case "", "debezium", "raw", "avro":
	default:
		*problems = append(*problems, fmt.Sprintf("source.format: unsupported %q (want debezium | raw | avro)", src.Format))
	}
	if src.Format == "raw" && src.Kind != "kafka" {
		*problems = append(*problems, "source.format: raw is only valid for kind kafka")
	}
	if src.Format == "avro" {
		if src.Kind != "kafka" {
			*problems = append(*problems, "source.format: avro is only valid for kind kafka")
		}
		if src.SchemaRegistry == "" {
			*problems = append(*problems, "source.schemaRegistry: required when format is avro (Confluent-compatible registry base URL)")
		}
	}
	switch src.OnDecodeError {
	case "", "fail", "skip":
	default:
		*problems = append(*problems, fmt.Sprintf("source.onDecodeError: unsupported %q (want fail | skip)", src.OnDecodeError))
	}
	if src.OnDecodeError != "" && src.Kind != "kafka" {
		*problems = append(*problems, "source.onDecodeError: only valid for kind kafka")
	}
	validateKafkaSecurity(src, problems)
	validateSchemaRegistryAuth(src, problems)
}

// validateKafkaSecurity checks the Kafka source's TLS/SASL block (issue #598).
// Both are optional; a broken block (a SASL mechanism with no credentials, or
// half a client certificate) is rejected here rather than failing at dial time.
func validateKafkaSecurity(src Source, problems *[]string) {
	if src.Kafka == nil {
		return
	}
	if src.Kind != "kafka" {
		*problems = append(*problems, "source.kafka: only valid for kind kafka")
	}
	if t := src.Kafka.TLS; t != nil {
		if (t.Cert == "") != (t.Key == "") {
			*problems = append(*problems, "source.kafka.tls: cert and key must be set together (client authentication needs both)")
		}
	}
	if s := src.Kafka.SASL; s != nil {
		switch s.Mechanism {
		case "", "plain", "scram-sha-256", "scram-sha-512":
		default:
			*problems = append(*problems, fmt.Sprintf("source.kafka.sasl.mechanism: unsupported %q (want plain | scram-sha-256 | scram-sha-512; AWS MSK IAM is not supported yet)", s.Mechanism))
		}
		if s.Username == "" || s.Password == "" {
			*problems = append(*problems, "source.kafka.sasl: username and password are required")
		}
	}
}

// validateSchemaRegistryAuth checks the optional schema-registry client auth
// (issue #604): basic auth needs both username and password, and a client
// certificate needs both cert and key.
func validateSchemaRegistryAuth(src Source, problems *[]string) {
	a := src.SchemaRegistryAuth
	if a == nil {
		return
	}
	if a.Username == "" || a.Password == "" {
		*problems = append(*problems, "source.schemaRegistryAuth: username and password are required")
	}
	if (a.Cert == "") != (a.Key == "") {
		*problems = append(*problems, "source.schemaRegistryAuth: cert and key must be set together")
	}
}
