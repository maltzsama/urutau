package spec

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v3"
)

// credentialEnv is the empty-field → env fallback contract for the
// Kubernetes path. The operator renders the spec into a ConfigMap without
// credentials and mounts the Secrets as environment variables; the
// coordinator resolves them here. A value inline in the spec always wins.
//
//	URUTAU_SOURCE_URI          → source.uri
//	URUTAU_SINK_URI            → sink.uri
//	URUTAU_SINK_CLIENT_ID      → sink.clientId
//	URUTAU_SINK_CLIENT_SECRET  → sink.clientSecret
//	URUTAU_SINK_SCOPE          → sink.scope
var credentialEnv = []struct {
	field func(*Spec) *string
	env   string
}{
	{func(s *Spec) *string { return &s.Source.URI }, "URUTAU_SOURCE_URI"},
	{func(s *Spec) *string { return &s.Sink.URI }, "URUTAU_SINK_URI"},
	{func(s *Spec) *string { return &s.Sink.ClientID }, "URUTAU_SINK_CLIENT_ID"},
	{func(s *Spec) *string { return &s.Sink.ClientSecret }, "URUTAU_SINK_CLIENT_SECRET"},
	{func(s *Spec) *string { return &s.Sink.Scope }, "URUTAU_SINK_SCOPE"},
}

// LoadYAML reads the inline YAML authoring format into a Spec. YAML is
// decoded into generic structures and round-tripped through JSON so the
// canonical json tags remain the single source of truth for field names.
// Credential and URI fields left empty are filled from the environment
// (credentialEnv); the k8s path relies on this.
func LoadYAML(r io.Reader) (*Spec, error) {
	var raw any
	if err := yaml.NewDecoder(r).Decode(&raw); err != nil {
		return nil, fmt.Errorf("spec: yaml: %w", err)
	}

	b, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("spec: encode: %w", err)
	}

	var s Spec
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("spec: decode: %w", err)
	}
	applyEnvFallback(&s)
	return &s, nil
}

// applyEnvFallback fills empty credential/URI fields from the environment.
// Inline values are never overridden.
func applyEnvFallback(s *Spec) {
	for _, c := range credentialEnv {
		p := c.field(s)
		if *p != "" {
			continue
		}
		if v, ok := os.LookupEnv(c.env); ok {
			*p = v
		}
	}
}
