package pods

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

// The pod e2e harness runs inside minikube as a Job (`make e2e-pods-test`,
// test/e2e/pods/run-in-cluster.sh), next to the pipeline it drives. There a
// `kubectl port-forward` would tunnel every MySQL transaction and Trino query
// through the API server: the load generator could not come near the full
// profile's rate. Instead, a "forward" in the cluster is an in-process TCP
// proxy on the same 127.0.0.1 port, dialing the target directly (a Service by
// its cluster DNS name, a Pod by its current IP). Every caller keeps using
// 127.0.0.1:<local>, in the cluster or on the host.

// inCluster reports whether the harness runs as the in-cluster Job.
func inCluster() bool { return os.Getenv("URUTAU_E2E_IN_CLUSTER") == "1" }

// clusterTarget resolves a port-forward resource to a dialable address:
// svc/<name> is the Service's cluster DNS name; pod/<name> is the Pod's IP now
// (looked up per connection, so a restarted Pod is followed).
func clusterTarget(ns, resource string, remote int) (string, error) {
	kind, name, ok := strings.Cut(resource, "/")
	if !ok {
		return "", fmt.Errorf("resource %q: want svc/<name> or pod/<name>", resource)
	}
	switch kind {
	case "svc", "service":
		return fmt.Sprintf("%s.%s.svc.cluster.local:%d", name, ns, remote), nil
	case "pod":
		ip, err := kubectlCmd("", "-n", ns, "get", "pod", name, "-o", "jsonpath={.status.podIP}")
		if err != nil {
			return "", err
		}
		if ip == "" {
			return "", fmt.Errorf("pod %s/%s has no IP yet", ns, name)
		}
		return net.JoinHostPort(ip, fmt.Sprint(remote)), nil
	default:
		return "", fmt.Errorf("resource %q: want svc/<name> or pod/<name>", resource)
	}
}

// startProxy listens on 127.0.0.1:local and pipes every connection to the
// resource's in-cluster address. It returns once the listener is up; cancel
// closes it and every open connection.
func startProxy(ns, resource string, local, remote int) (context.CancelFunc, error) {
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", local))
	if err != nil {
		return nil, fmt.Errorf("proxy %s on :%d: %w", resource, local, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	conns := map[net.Conn]bool{}
	track := func(c net.Conn, add bool) {
		mu.Lock()
		defer mu.Unlock()
		if add {
			conns[c] = true
		} else {
			delete(conns, c)
		}
	}
	go func() {
		<-ctx.Done()
		_ = ln.Close()
		mu.Lock()
		for c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
	}()
	go func() {
		for {
			in, err := ln.Accept()
			if err != nil {
				return // closed by cancel
			}
			go func() {
				defer func() { _ = in.Close() }()
				addr, err := clusterTarget(ns, resource, remote)
				if err != nil {
					return
				}
				out, err := net.DialTimeout("tcp", addr, 10*time.Second)
				if err != nil {
					return
				}
				defer func() { _ = out.Close() }()
				track(in, true)
				track(out, true)
				defer track(in, false)
				defer track(out, false)
				done := make(chan struct{}, 2)
				go func() { _, _ = io.Copy(out, in); done <- struct{}{} }()
				go func() { _, _ = io.Copy(in, out); done <- struct{}{} }()
				<-done
			}()
		}
	}()
	return cancel, nil
}
