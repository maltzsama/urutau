package spec

import (
	"encoding/json"
	"fmt"
	"io"

	"gopkg.in/yaml.v3"
)

// LoadYAML reads the inline YAML authoring format into a Spec. YAML is
// decoded into generic structures and round-tripped through JSON so the
// canonical json tags remain the single source of truth for field names.
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
	return &s, nil
}
