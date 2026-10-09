package decoder

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
)

// #604: the schema-registry client sends HTTP basic auth when configured.
func TestHTTPRegistrySendsBasicAuth(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"schema":"\"string\""}`))
	}))
	defer srv.Close()

	reg := NewHTTPRegistryWithAuth(srv.URL, "user", "pass", nil)
	if _, err := reg.Get(context.Background(), 1); err != nil {
		t.Fatalf("Get: %v", err)
	}
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:pass"))
	if gotAuth != want {
		t.Fatalf("Authorization = %q, want %q", gotAuth, want)
	}
}

// Without auth, no Authorization header is sent.
func TestHTTPRegistryNoAuthHeader(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"schema":"\"string\""}`))
	}))
	defer srv.Close()

	reg := NewHTTPRegistry(srv.URL)
	if _, err := reg.Get(context.Background(), 1); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if gotAuth != "" {
		t.Fatalf("Authorization = %q, want empty", gotAuth)
	}
}
