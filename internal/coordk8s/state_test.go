package coordk8s

import (
	"testing"

	"github.com/maltzsama/urutau/spec"
)

func TestWorkerPodTemplateHelpers(t *testing.T) {
	if got := spec.WorkerPodTemplateKey("raw.orders"); got != "worker-pod-template.raw.orders.yaml" {
		t.Fatalf("workerPodTemplateFile = %q", got)
	}
	if got := anyTarget(nil); got != "" {
		t.Fatalf("anyTarget(nil) = %q", got)
	}
	if got := anyTarget(map[string]string{"a": "x"}); got != "x" {
		t.Fatalf("anyTarget = %q", got)
	}
	if WorkerPodTemplateAvailable(nil) {
		t.Fatal("no targets means no template")
	}
	if WorkerPodTemplateAvailable(map[string]string{"a": "x"}) {
		t.Fatal("a template file that does not exist must report unavailable")
	}
	if _, err := LoadWorkerPodTemplate("missing"); err == nil {
		t.Fatal("a missing template file must error")
	}
	// A discovered target with no own template falls back to the generic one.
	cands := workerPodTemplateCandidates("raw.discovered")
	if len(cands) != 2 || cands[0] != "worker-pod-template.raw.discovered.yaml" ||
		cands[1] != "worker-pod-template."+spec.DefaultWorkerTemplateTarget+".yaml" {
		t.Fatalf("candidates = %v, want the table's own then the generic", cands)
	}
}
