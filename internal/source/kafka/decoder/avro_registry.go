package decoder

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/hamba/avro/v2"
)

// SchemaRegistry resolves Avro schemas by Confluent schema id. Schemas are
// immutable once registered under an id — evolution publishes a NEW id and
// never rewrites an old one — so a per-id cache has no staleness window and
// no expiry.
type SchemaRegistry interface {
	// Get returns the schema registered under id. A registry that does not
	// know the id returns ErrUnknownSchema.
	Get(ctx context.Context, id int) (avro.Schema, error)
}

// ErrUnknownSchema marks a schema id the registry does not know — the
// producer pointed at a different registry, or the id is corrupt. There is
// no schema to retry, so this is a needs-human condition, not a transient
// one.
type ErrUnknownSchema struct {
	ID int
}

func (e *ErrUnknownSchema) Error() string {
	return fmt.Sprintf("schema registry: unknown schema id %d", e.ID)
}

// HTTPRegistry is a Confluent-compatible schema registry client. Schemas are
// cached by id for the life of the process (immutable per id).
type HTTPRegistry struct {
	base   string
	client *http.Client
	cache  sync.Map // int -> avro.Schema
}

// NewHTTPRegistry dials the registry at base (no leading path requirement).
func NewHTTPRegistry(base string) *HTTPRegistry {
	return &HTTPRegistry{
		base:   base,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// Get resolves a schema by id, hitting the registry on a cache miss.
func (r *HTTPRegistry) Get(ctx context.Context, id int) (avro.Schema, error) {
	if s, ok := r.cache.Load(id); ok {
		return s.(avro.Schema), nil
	}
	schema, err := r.fetch(ctx, id)
	if err != nil {
		return nil, err
	}
	r.cache.Store(id, schema)
	return schema, nil
}

type registrySchemaResponse struct {
	Schema string `json:"schema"`
}

// fetch downloads and parses the schema for one id.
func (r *HTTPRegistry) fetch(ctx context.Context, id int) (avro.Schema, error) {
	url := fmt.Sprintf("%s/schemas/ids/%d", r.base, id)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil, &ErrUnknownSchema{ID: id}
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		return nil, fmt.Errorf("schema registry: GET %s: %s", url, http.StatusText(resp.StatusCode))
	}
	var parsed registrySchemaResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("schema registry: decode response for id %d: %w", id, err)
	}
	schema, err := avro.Parse(parsed.Schema)
	if err != nil {
		return nil, fmt.Errorf("schema registry: parse schema id %d: %w", id, err)
	}
	return schema, nil
}
