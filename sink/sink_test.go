package sink

import (
	"context"
	"log/slog"
	"strings"
	"testing"
)

// R2: a logged Config must not leak credentials — the Options map carries
// client_secret and friends.
func TestConfigLogValueRedactsSecrets(t *testing.T) {
	c := Config{
		Type:      "iceberg+rest",
		URI:       "https://catalog",
		Namespace: "raw",
		Options: map[string]string{
			"warehouse":     "wh",
			"client_id":     "the-id",
			"client_secret": "SUPER-SECRET",
			"scope":         "PRINCIPAL_ROLE:ALL",
		},
	}
	var sb strings.Builder
	slog.New(slog.NewTextHandler(&sb, nil)).Info("cfg", "config", c)
	out := sb.String()
	if strings.Contains(out, "SUPER-SECRET") {
		t.Fatalf("secret leaked into the log: %s", out)
	}
	if !strings.Contains(out, "[REDACTED]") {
		t.Fatalf("secret key not redacted: %s", out)
	}
	if !strings.Contains(out, "the-id") {
		t.Fatalf("non-secret option should survive: %s", out)
	}
}

var _ = context.Background
