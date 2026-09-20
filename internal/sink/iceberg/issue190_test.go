package iceberg

import (
	"context"
	"strings"
	"testing"

	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/sink"
)

// #190: an empty client id/secret must fail fast, not build the ":" credential.
func TestOpenRejectsEmptyCredentials(t *testing.T) {
	_, err := Open(context.Background(), sink.Config{
		URI:       "http://localhost:8181/api/catalog",
		Namespace: "raw",
		Options:   map[string]string{driver.OptWarehouse: "wh"},
	})
	if err == nil || !strings.Contains(err.Error(), "clientId") {
		t.Fatalf("Open with empty credentials = %v, want a clientId/clientSecret error", err)
	}
}
