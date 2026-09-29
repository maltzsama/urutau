package operator

// The coordinator's run trail (--eventlog): the store it writes to, and the
// credentials only the coordinator's container carries (#465).

import (
	"strings"

	corev1 "k8s.io/api/core/v1"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
)

// eventlogArgs renders the CR's eventlog spec as coordinator flags: none when
// it is unset, the endpoint only for an S3-compatible store.
func eventlogArgs(ev *urutauv1alpha1.EventlogSpec) []string {
	if ev == nil || ev.Bucket == "" {
		return nil
	}
	args := []string{"--eventlog", eventlogURI(ev)}
	if ev.Endpoint != "" {
		args = append(args, "--eventlog-endpoint", ev.Endpoint)
	}
	return args
}

// eventlogURI renders the CR's eventlog spec as the coordinator's
// s3://<bucket>/<prefix> URI.
func eventlogURI(ev *urutauv1alpha1.EventlogSpec) string {
	prefix := strings.Trim(ev.RootPrefix, "/")
	if prefix == "" {
		return "s3://" + ev.Bucket
	}
	return "s3://" + ev.Bucket + "/" + prefix
}

// coordinatorContainerEnv is coordinatorEnv plus the eventlog store's
// credentials (#465): the coordinator writes the trail, the workers do not, so
// only its container carries them.
func coordinatorContainerEnv(cr *urutauv1alpha1.CDCPipeline) []corev1.EnvVar {
	env := coordinatorEnv(cr)
	ev := cr.Spec.Coordinator.Eventlog
	if ev == nil || ev.Bucket == "" || ev.Secret == "" {
		return env
	}
	for _, kv := range []struct{ env, key string }{
		{"AWS_ACCESS_KEY_ID", "accessKeyId"},
		{"AWS_SECRET_ACCESS_KEY", "secretAccessKey"},
	} {
		env = append(env, corev1.EnvVar{
			Name: kv.env,
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: ev.Secret},
				Key:                  kv.key,
			}},
		})
	}
	return env
}
