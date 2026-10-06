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
}
