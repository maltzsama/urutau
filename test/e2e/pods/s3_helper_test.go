package pods

import (
	"strings"
	"testing"
	"time"
)

// The S3 helper Pod runs in the middle of a run, on a node that has pulled
// its image already. With a :latest tag the kubelet pulls again on every
// start, and kubectl run waits one minute for the Pod: a new upstream image
// took 46 s to pull and 20 s to start, and a 53-minute run was lost to a
// timeout (#853). The helper must use the image the node has and wait long
// enough for a loaded node.
func TestS3HelperDoesNotDependOnARegistryPull(t *testing.T) {
	args := s3RunArgs("pr-s3-test", "true")
	var policy, timeout string
	for _, a := range args {
		if a == "--" {
			break // the rest is the Pod's command
		}
		if v, ok := strings.CutPrefix(a, "--image-pull-policy="); ok {
			policy = v
		}
		if v, ok := strings.CutPrefix(a, "--pod-running-timeout="); ok {
			timeout = v
		}
	}
	if policy != "IfNotPresent" {
		t.Errorf("image pull policy = %q, want IfNotPresent: a :latest image is pulled from the registry on every start", policy)
	}
	d, err := time.ParseDuration(timeout)
	if err != nil || d < 3*time.Minute {
		t.Errorf("pod running timeout = %q, want at least 3m: kubectl's default minute is less than a pull and a start on a loaded node", timeout)
	}
}
