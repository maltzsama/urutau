package operator

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

// Leader election coordinates the manager through a coordination.k8s.io Lease.
// Without the leases rule the manager crash-loops at startup on lease
// acquisition, so guard the shipped ClusterRole against losing it.
func TestOperatorClusterRoleGrantsLeases(t *testing.T) {
	path := filepath.Join("..", "..", "config", "rbac", "operator.yaml")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	// The file holds the ClusterRole and the ClusterRoleBinding; parse the
	// first document only.
	first := strings.Split(string(b), "\n---")[0]
	var role rbacv1.ClusterRole
	if err := yaml.Unmarshal([]byte(first), &role); err != nil {
		t.Fatalf("unmarshal ClusterRole: %v", err)
	}
	for _, rule := range role.Rules {
		if !slices.Contains(rule.APIGroups, "coordination.k8s.io") || !slices.Contains(rule.Resources, "leases") {
			continue
		}
		for _, v := range []string{"get", "list", "watch", "create", "update", "patch"} {
			if !slices.Contains(rule.Verbs, v) {
				t.Fatalf("leases rule missing verb %q: %v", v, rule.Verbs)
			}
		}
		return
	}
	t.Fatal("ClusterRole grants no coordination.k8s.io/leases rule — leader election would crash-loop")
}
