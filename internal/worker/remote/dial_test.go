package remote

import (
	"testing"

	"github.com/maltzsama/urutau/internal/grpctls"
)

// #265: dialOpts must return an error when TLS is enabled but the client
// credentials are invalid, not silently dial plaintext.
func TestDialOptsRejectsBadTLS(t *testing.T) {
	_, err := dialOpts(grpctls.Config{CertFile: "/nonexistent", KeyFile: "/nonexistent", ClientCAFile: "/nonexistent"})
	if err == nil {
		t.Fatal("dialOpts must error on invalid TLS credentials, not fall back to plaintext")
	}
}
