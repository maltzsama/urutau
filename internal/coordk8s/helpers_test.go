package coordk8s

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// sampleTemplate is a worker Pod template as the operator renders it.
func sampleTemplate() corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "urutau-worker"}},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "worker",
				Image: "urutau:v1",
				Args:  []string{"run", "--coordinator", "orders-coordinator:50051"},
			}},
		},
	}
}
