package operator

import (
	"bytes"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/yaml"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
	urutauspec "github.com/maltzsama/urutau/spec"
)

// resourceRequirements builds a container's resource request/limit from
// Kubernetes quantity strings. cpu/memory become the request; cpu+overhead/
// memory+overhead become the limit — the request alone when no overhead is
// given (limit == request), matching CoordinatorSpec, which has no overhead
// knob. Empty cpu/memory fall back to defaultCPU/defaultMemory, so the result
// is never empty (a pod is never BestEffort); overhead still adds only to the
// limit. The quantities are validated at admission and in validateSpec, so
// MustParse cannot see a bad value (issue #575).
func resourceRequirements(cpu, cpuOverhead, memory, memOverhead string) corev1.ResourceRequirements {
	if cpu == "" {
		cpu = defaultCPU
	}
	if memory == "" {
		memory = defaultMemory
	}
	req := corev1.ResourceList{}
	lim := corev1.ResourceList{}
	req[corev1.ResourceCPU] = resource.MustParse(cpu)
	lim[corev1.ResourceCPU] = addQuantity(cpu, cpuOverhead)
	req[corev1.ResourceMemory] = resource.MustParse(memory)
	lim[corev1.ResourceMemory] = addQuantity(memory, memOverhead)
	return corev1.ResourceRequirements{Requests: req, Limits: lim}
}

// addQuantity adds an optional overhead quantity to a base quantity;
// overhead=="" returns base unchanged (limit == request).
func addQuantity(base, overhead string) resource.Quantity {
	b := resource.MustParse(base)
	if overhead == "" {
		return b
	}
	o := resource.MustParse(overhead)
	b.Add(o)
	return b
}

// workerResources resolves one table's effective worker resources: the
// table's own spec.Table.Workers.CPU/Memory when set, else the pipeline-wide
// spec.worker default — matching CoordinatorSpec.CPU/Memory, a table-level
// override always wins over the fallback. The default carries
// WorkerDefaults' overhead; a table override does not declare its own
// overhead (spec.WorkerSpec has no overhead field), so it inherits the
// pipeline default's overhead too.
func workerResources(cr *urutauv1alpha1.CDCPipeline, t urutauspec.Table) corev1.ResourceRequirements {
	wd := cr.Spec.Worker
	cpu, memory := wd.CPU, wd.Memory
	if t.Workers != nil {
		if t.Workers.CPU != "" {
			cpu = t.Workers.CPU
		}
		if t.Workers.Memory != "" {
			memory = t.Workers.Memory
		}
	}
	return resourceRequirements(cpu, wd.CPUOverhead, memory, wd.MemoryOverhead)
}

// validateResourceQuantities rejects a CR whose resource fields are not valid
// Kubernetes quantities. resourceRequirements parses them with
// resource.MustParse at reconcile time, which panics on a bad value; without
// this check a CR like `memory: "2GB"` passed admission and panicked every
// reconcile of that pipeline (issue #575).
func validateResourceQuantities(cr *urutauv1alpha1.CDCPipeline) error {
	check := func(path, v string) error {
		if v == "" {
			return nil
		}
		if _, err := resource.ParseQuantity(v); err != nil {
			return fmt.Errorf("%s: %q is not a valid resource quantity: %w", path, v, err)
		}
		return nil
	}
	co, wd := cr.Spec.Coordinator, cr.Spec.Worker
	for _, f := range []struct{ path, v string }{
		{"coordinator.cpu", co.CPU},
		{"coordinator.memory", co.Memory},
		{"worker.cpu", wd.CPU},
		{"worker.cpuOverhead", wd.CPUOverhead},
		{"worker.memory", wd.Memory},
		{"worker.memoryOverhead", wd.MemoryOverhead},
	} {
		if err := check(f.path, f.v); err != nil {
			return err
		}
	}
	// A table's own workers.cpu/memory override the worker defaults and reach
	// resourceRequirements the same way, so validate them too.
	if len(cr.Spec.Definition.Inline) == 0 {
		return nil
	}
	b, err := yaml.Marshal(cr.Spec.Definition.Inline)
	if err != nil {
		return nil // the inline spec's shape is validated elsewhere
	}
	s, err := urutauspec.LoadYAML(bytes.NewReader(b))
	if err != nil {
		return nil
	}
	for i, t := range s.Tables {
		if t.Workers == nil {
			continue
		}
		if err := check(fmt.Sprintf("tables[%d].workers.cpu", i), t.Workers.CPU); err != nil {
			return err
		}
		if err := check(fmt.Sprintf("tables[%d].workers.memory", i), t.Workers.Memory); err != nil {
			return err
		}
	}
	return nil
}
