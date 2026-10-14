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

// Leader election coordinates the manager through a coordination.k8s.io Lease
// in the operator's own namespace. A namespace-scoped install binds the
// ClusterRole only into the watched namespaces, so the Lease must be granted
// by a Role bound in the operator's namespace; without it the manager retries
// the Lease forever and never reconciles (issue #867). Guard the shipped RBAC
// against losing it.
func TestOperatorLeaderElectionRoleGrantsLeases(t *testing.T) {
	const ns, name, sa = "urutau-system", "urutau-operator-leader-election", "urutau-operator"
	path := filepath.Join("..", "..", "config", "rbac", "operator.yaml")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var role *rbacv1.Role
	var binding *rbacv1.RoleBinding
	for _, doc := range strings.Split(string(b), "\n---") {
		var meta struct{ Kind string }
		if err := yaml.Unmarshal([]byte(doc), &meta); err != nil {
			t.Fatalf("unmarshal %s: %v", path, err)
		}
		switch meta.Kind {
		case "Role":
			role = &rbacv1.Role{}
			if err := yaml.Unmarshal([]byte(doc), role); err != nil {
				t.Fatalf("unmarshal Role: %v", err)
			}
		case "RoleBinding":
			binding = &rbacv1.RoleBinding{}
			if err := yaml.Unmarshal([]byte(doc), binding); err != nil {
				t.Fatalf("unmarshal RoleBinding: %v", err)
			}
		}
	}
	if role == nil || role.Name != name || role.Namespace != ns {
		t.Fatalf("no Role %s/%s in %s", ns, name, path)
	}
	if binding == nil || binding.Namespace != ns || binding.RoleRef.Kind != "Role" || binding.RoleRef.Name != name ||
		!slices.ContainsFunc(binding.Subjects, func(s rbacv1.Subject) bool {
			return s.Kind == "ServiceAccount" && s.Name == sa && s.Namespace == ns
		}) {
		t.Fatalf("Role %s is not bound to %s/%s in %s: %+v", name, ns, sa, ns, binding)
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
	t.Fatal("leader-election Role grants no coordination.k8s.io/leases rule — the manager would never become leader")
}
