package observability

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"sigs.k8s.io/yaml"
)

// The shipped PrometheusRule must reference only metrics this package
// declares — a renamed metric would make an alert silently never fire. Guards
// config/monitoring/prometheusrule.yaml against drift from metrics.go.
func TestPrometheusRuleAlertsUseDeclaredMetrics(t *testing.T) {
	src, err := os.ReadFile("metrics.go")
	if err != nil {
		t.Fatalf("read metrics.go: %v", err)
	}
	declared := map[string]bool{}
	for _, m := range regexp.MustCompile(`Name:\s*"(urutau_[a-z0-9_]+)"`).FindAllStringSubmatch(string(src), -1) {
		declared[m[1]] = true
	}
	if len(declared) == 0 {
		t.Fatal("no urutau_* metric names found in metrics.go — regex drift")
	}

	path := filepath.Join("..", "..", "config", "monitoring", "prometheusrule.yaml")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var rule struct {
		Spec struct {
			Groups []struct {
				Rules []struct {
					Alert string `json:"alert"`
					Expr  string `json:"expr"`
				} `json:"rules"`
			} `json:"groups"`
		} `json:"spec"`
	}
	if err := yaml.Unmarshal(b, &rule); err != nil {
		t.Fatalf("unmarshal PrometheusRule: %v", err)
	}

	re := regexp.MustCompile(`urutau_[a-z0-9_]+`)
	var alerts, refs int
	for _, g := range rule.Spec.Groups {
		for _, r := range g.Rules {
			alerts++
			for _, name := range re.FindAllString(r.Expr, -1) {
				refs++
				if !declared[name] {
					t.Errorf("alert %q references undeclared metric %q: %s", r.Alert, name, r.Expr)
				}
			}
		}
	}
	if alerts == 0 || refs == 0 {
		t.Fatalf("no alerts/metrics parsed (alerts=%d refs=%d) — file or schema drift", alerts, refs)
	}
}
