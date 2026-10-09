package spec

// KafkaSource configures a Kafka source's transport security (issue #598).
// Both fields are optional: nil means the corresponding protection is off —
// the connection is plaintext, or unauthenticated.
type KafkaSource struct {
	TLS  *KafkaTLS  `json:"tls,omitempty"`
	SASL *KafkaSASL `json:"sasl,omitempty"`
}

// KafkaTLS secures the broker connection. CA, Cert and Key are file PATHS to
// PEM material (the operator mounts them from a Secret, like the SSH key), not
// inline PEM. CA alone gives server verification; Cert+Key add client (mutual)
// authentication. InsecureSkipVerify disables server verification and is only
// for a test broker with a self-signed certificate.
type KafkaTLS struct {
	CA                 string `json:"ca,omitempty"`
	Cert               string `json:"cert,omitempty"`
	Key                string `json:"key,omitempty"`
	InsecureSkipVerify bool   `json:"insecureSkipVerify,omitempty"`
}

// KafkaSASL authenticates to the broker. Mechanism is "plain",
// "scram-sha-256" or "scram-sha-512". AWS MSK IAM is not supported yet.
type KafkaSASL struct {
	Mechanism string `json:"mechanism,omitempty"`
	Username  string `json:"username,omitempty"`
	Password  string `json:"password,omitempty"`
}

// SchemaRegistryAuth configures the Confluent-compatible schema-registry
// client's HTTP authentication and TLS (issue #604). Both are optional:
// Confluent Cloud uses basic auth (an API key/secret) over TLS. CA, Cert and
// Key are file paths, like the broker's TLS material.
type SchemaRegistryAuth struct {
	Username           string `json:"username,omitempty"`
	Password           string `json:"password,omitempty"`
	CA                 string `json:"ca,omitempty"`
	Cert               string `json:"cert,omitempty"`
	Key                string `json:"key,omitempty"`
	InsecureSkipVerify bool   `json:"insecureSkipVerify,omitempty"`
}
