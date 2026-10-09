package proc

import (
	"strings"
	"testing"
)

// #672: the plugin must not inherit the parent's credentials/DSNs; only the
// base allowlist and the URUTAU_* contract variables are forwarded.
func TestPluginEnvDropsParentCredentials(t *testing.T) {
	t.Setenv("AWS_SECRET_ACCESS_KEY", "super-secret")
	t.Setenv("URUTAU_SOURCE_URI", "postgres://user:pw@host/db")
	t.Setenv("PATH", "/usr/bin")

	env := pluginEnv(Config{Role: "source", Token: "tok", ConfigPath: "/c", PluginDir: "/p"}, "/tmp/s.sock", false)
	joined := strings.Join(env, "\n")
	for _, leaked := range []string{"super-secret", "AWS_SECRET_ACCESS_KEY", "URUTAU_SOURCE_URI", "user:pw"} {
		if strings.Contains(joined, leaked) {
			t.Fatalf("parent credential %q leaked into the plugin env: %v", leaked, env)
		}
	}
	for _, want := range []string{"URUTAU_TOKEN=tok", "PATH=/usr/bin", "URUTAU_STAGE=source", "URUTAU_SOCKET=/tmp/s.sock"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("required env %q missing: %v", want, env)
		}
	}
}
