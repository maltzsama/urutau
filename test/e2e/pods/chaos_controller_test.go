package pods

// The nondeterministic Chaos Mesh controller (issue #385): while the
// production-readiness workload (#384) flows through the pipeline, it keeps
// drawing a fault type, a target, a start time, a duration and whether to
// overlap the fault with others, and injects each as a real Chaos Mesh
// experiment against the live coordinator and worker Pods. Every experiment
// is recorded (type, target, times, injection outcome, and the pipeline's
// state when it started) for diagnosis; the record is never replayed as a
// schedule.
//
// The controller only ever creates Chaos Mesh resources. It never deletes a
// Pod or signals a process itself.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// chaosKind is one fault the controller can inject.
type chaosKind string

const (
	chaosWorkerKill       chaosKind = "worker-pod-kill"
	chaosCoordinatorKill  chaosKind = "coordinator-pod-kill"
	chaosWorkerFailure    chaosKind = "worker-pod-failure"
	chaosCoordFailure     chaosKind = "coordinator-pod-failure"
	chaosNetworkPartition chaosKind = "network-partition"
	chaosNetworkLoss      chaosKind = "network-loss"
	chaosNetworkDelay     chaosKind = "network-delay"
	chaosCPUStress        chaosKind = "cpu-stress"
	chaosMemoryStress     chaosKind = "memory-stress"
)

// allChaosKinds lists every kind, in a fixed order for the planner.
var allChaosKinds = []chaosKind{
	chaosWorkerKill, chaosCoordinatorKill, chaosWorkerFailure, chaosCoordFailure,
	chaosNetworkPartition, chaosNetworkLoss, chaosNetworkDelay, chaosCPUStress, chaosMemoryStress,
}

// chaosProfile sizes the fault stream.
type chaosProfile struct {
	MeanGap       time.Duration // mean time between two injections (exponential)
	MinDuration   time.Duration // shortest duration-bound fault
	MaxDuration   time.Duration // longest duration-bound fault
	MaxConcurrent int           // most experiments active at once
	// Weights biases the kind draw; a kind absent from the map is never drawn.
	Weights map[chaosKind]float64
}

var (
	smokeChaos = chaosProfile{
		MeanGap: 20 * time.Second, MinDuration: 5 * time.Second, MaxDuration: 30 * time.Second, MaxConcurrent: 2,
		Weights: map[chaosKind]float64{
			chaosWorkerKill: 2, chaosCoordinatorKill: 1, chaosWorkerFailure: 1, chaosCoordFailure: 0.5,
			chaosNetworkPartition: 1, chaosNetworkLoss: 1, chaosNetworkDelay: 1, chaosCPUStress: 1, chaosMemoryStress: 1,
		},
	}
	fullChaos = chaosProfile{
		MeanGap: 30 * time.Second, MinDuration: 10 * time.Second, MaxDuration: 2 * time.Minute, MaxConcurrent: 3,
		Weights: smokeChaos.Weights,
	}
)

// chaosProfileFor pairs a chaos profile with the workload profile.
func chaosProfileFor(p workloadProfile) chaosProfile {
	if p.Name == fullProfile.Name {
		return fullChaos
	}
	return smokeChaos
}

// chaosScope is how wide a worker-side fault reaches.
type chaosScope string

const (
	scopeOnePod      chaosScope = "one-pod"       // one Pod, picked at injection time
	scopeTableGroup  chaosScope = "table-workers" // every worker of one table
	scopeAllWorkers  chaosScope = "all-workers"   // every worker of the pipeline
	scopeCoordinator chaosScope = "coordinator"
)

// chaosDecision is one draw of the planner: what to inject, how long after
// the previous decision, for how long, and against which kind of target.
// The concrete Pod is resolved only when the fault is injected, because
// scaling and re-slicing change the Pod set.
type chaosDecision struct {
	Kind     chaosKind
	Delay    time.Duration // wait before this injection
	Duration time.Duration // for duration-bound kinds
	Scope    chaosScope
	Overlap  bool   // may start while other experiments are active
	Pick     uint64 // random value that selects the concrete target
	// Kind-specific parameters.
	LossPercent int           // network-loss
	Latency     time.Duration // network-delay
	CPUWorkers  int           // cpu-stress
	MemoryMB    int           // memory-stress
}

// chaosPlanner draws decisions from a seeded stream. It is pure, so the
// draws are unit-tested without a cluster.
type chaosPlanner struct {
	r *rand.Rand
	p chaosProfile
}

func newChaosPlanner(seed uint64, p chaosProfile) *chaosPlanner {
	return &chaosPlanner{r: rand.New(rand.NewPCG(seed, 0xC4A05)), p: p}
}

func (pl *chaosPlanner) kind() chaosKind {
	total := 0.0
	for _, k := range allChaosKinds {
		total += pl.p.Weights[k]
	}
	x := pl.r.Float64() * total
	for _, k := range allChaosKinds {
		x -= pl.p.Weights[k]
		if x < 0 {
			return k
		}
	}
	return allChaosKinds[len(allChaosKinds)-1]
}

func (pl *chaosPlanner) next() chaosDecision {
	d := chaosDecision{
		Kind:    pl.kind(),
		Delay:   time.Duration(pl.r.ExpFloat64() * float64(pl.p.MeanGap)),
		Overlap: pl.r.Float64() < 0.5,
		Pick:    pl.r.Uint64(),
	}
	span := float64(pl.p.MaxDuration - pl.p.MinDuration)
	d.Duration = pl.p.MinDuration + time.Duration(pl.r.Float64()*span)
	switch d.Kind {
	case chaosCoordinatorKill, chaosCoordFailure:
		d.Scope = scopeCoordinator
	case chaosWorkerKill, chaosWorkerFailure:
		d.Scope = scopeOnePod
	case chaosNetworkPartition, chaosNetworkLoss, chaosNetworkDelay:
		// Between workers and the coordinator: one table's workers or all.
		d.Scope = scopeTableGroup
		if pl.r.Float64() < 0.3 {
			d.Scope = scopeAllWorkers
		}
	case chaosCPUStress, chaosMemoryStress:
		// Stress lands on a worker or, sometimes, the coordinator.
		d.Scope = scopeOnePod
		if pl.r.Float64() < 0.25 {
			d.Scope = scopeCoordinator
		}
	}
	d.LossPercent = 10 + pl.r.IntN(81)                                           // 10–90 %
	d.Latency = time.Duration(50+pl.r.IntN(951)) * time.Millisecond              // 50–1000 ms
	d.CPUWorkers = 1 + pl.r.IntN(2)                                              // 1–2
	d.MemoryMB = 128 * (1 + pl.r.IntN(4))                                        // 128–512 MB
	d.Delay = time.Duration(math.Min(float64(d.Delay), float64(4*pl.p.MeanGap))) // no silent stretch
	return d
}

// ── recording ───────────────────────────────────────────────────────────

// chaosState is the pipeline's state when an experiment starts.
type chaosState struct {
	Pods           map[string]string `json:"pods"`           // name → "phase restarts=N"
	WorkerReplicas map[string]int    `json:"workerReplicas"` // worker StatefulSet → replicas (KEDA)
	Maintenance    int               `json:"maintenancePods"`
	Workload       string            `json:"workload"`
	ActiveChaos    []string          `json:"activeChaos"`
}

// chaosEvent is one experiment the controller decided on: injected, failed
// to inject (Error), or — a reactive network fault while another is active —
// skipped (Skipped), in which case only Kind, Trigger, Requested and Skipped
// are set: no resource was created.
type chaosEvent struct {
	Seq        int           `json:"seq"`
	Trigger    string        `json:"trigger"` // "planner", or what a reactive injection answered
	Kind       chaosKind     `json:"kind"`
	Resource   string        `json:"resource"` // podchaos / networkchaos / stresschaos
	Name       string        `json:"name"`
	Scope      chaosScope    `json:"scope"`
	Target     string        `json:"target"`
	Params     string        `json:"params,omitempty"`
	Planned    time.Duration `json:"plannedDuration"`
	Requested  time.Time     `json:"requested"`
	InjectedAt time.Time     `json:"injectedAt,omitzero"`
	RemovedAt  time.Time     `json:"removedAt,omitzero"`
	Injected   bool          `json:"injected"`
	Reapplied  int           `json:"reapplied,omitempty"` // network fault re-created before it injected
	Error      string        `json:"error,omitempty"`
	// Skipped is why a reactive fault was not injected at all (not a
	// failure: the record shows the transition was seen).
	Skipped string     `json:"skipped,omitempty"`
	State   chaosState `json:"state"`
}

// ── executor ────────────────────────────────────────────────────────────

// chaosController runs the planner against the live pipeline.
type chaosController struct {
	ns, pipeline string
	seed         uint64
	profile      chaosProfile
	planner      *chaosPlanner
	reactive     *chaosPlanner // draws for injectNow, apart from the main stream
	workload     func() string // the workload's phase, for the record

	mu       sync.Mutex
	stopping bool // set by stop under mu before it waits: injectNow then refuses
	seq      int  // experiment counter, shared by the planner loop and injectNow
	events   []*chaosEvent
	active   map[string]string // CR name → resource kind, still present
	// network holds the active experiments that are network faults. Chaos
	// Mesh cannot stack two NetworkChaos on one Pod (the second fails with
	// "unable to flush ip sets"), and every network fault can reach every
	// Pod, so they never overlap.
	network map[string]bool

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func newChaosController(ns, pipeline string, seed uint64, p chaosProfile, workload func() string) *chaosController {
	return &chaosController{
		ns: ns, pipeline: pipeline, seed: seed, profile: p,
		planner: newChaosPlanner(seed, p), reactive: newChaosPlanner(seed^0x5EAC7, p), workload: workload,
		active: map[string]string{}, network: map[string]bool{},
	}
}

// isNetworkFault reports whether kind is a NetworkChaos experiment.
func isNetworkFault(kind chaosKind) bool {
	return kind == chaosNetworkPartition || kind == chaosNetworkLoss || kind == chaosNetworkDelay
}

// admit reports whether a planner draw may start now, and reserves its slot
// when it may. Caller holds c.mu.
func (c *chaosController) admit(d chaosDecision) (seq int, ok bool) {
	n := len(c.active)
	if n >= c.profile.MaxConcurrent || (n > 0 && !d.Overlap) {
		return 0, false
	}
	if isNetworkFault(d.Kind) && len(c.network) > 0 {
		return 0, false
	}
	return c.reserve(d.Kind), true
}

// reserve takes the next experiment slot for kind. Caller holds c.mu.
func (c *chaosController) reserve(kind chaosKind) int {
	c.seq++
	name := c.name(c.seq)
	c.active[name] = reserved
	if isNetworkFault(kind) {
		c.network[name] = true
	}
	return c.seq
}

// kubectlTimeout bounds one controller kubectl call.
const kubectlTimeout = 2 * time.Minute

// kubectlCmd runs kubectl without a *testing.T, for the controller's own
// goroutines: a failure is an error to record, not a test abort.
func kubectlCmd(stdin string, args ...string) (string, error) {
	return kubectlCmdBy(time.Now().Add(kubectlTimeout), stdin, args...)
}

// kubectlCmdBy is kubectlCmd bounded by until instead of kubectlTimeout.
func kubectlCmdBy(until time.Time, stdin string, args ...string) (string, error) {
	// Bounded: a hung API server or deletion must not hang the experiment
	// goroutine, and with it stop() and the test's cleanup.
	ctx, cancel := context.WithDeadline(context.Background(), until)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var out, errb strings.Builder
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("kubectl %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(errb.String()))
	}
	return strings.TrimSpace(out.String()), nil
}

// start runs the decision loop until stop.
func (c *chaosController) start(ctx context.Context) {
	c.stopCh = make(chan struct{})
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			d := c.planner.next()
			select {
			case <-c.stopCh:
				return
			case <-ctx.Done():
				return
			case <-time.After(d.Delay):
			}
			// Check and reserve under one lock: an injection counts as
			// active from here, before its resource exists, so two quick
			// draws cannot both pass the limit.
			c.mu.Lock()
			seq, ok := c.admit(d)
			c.mu.Unlock()
			if !ok {
				continue // this draw would overlap more than allowed: skip it
			}
			c.wg.Add(1)
			go func(seq int, d chaosDecision) {
				defer c.wg.Done()
				c.inject(ctx, seq, d, "planner")
			}(seq, d)
		}
	}()
}

// injectNow injects one fault of kind right away, outside the planner's
// timing: a hook that sees a partition transition start uses it so a fault
// overlaps the transition by construction. The target and parameters are
// still drawn at random, and the experiment is recorded like any other,
// with trigger naming what caused it. It is a no-op once stop has begun.
func (c *chaosController) injectNow(ctx context.Context, kind chaosKind, trigger string) {
	// Checked and counted under the lock stop takes before it waits, so a
	// reactive injection either joins the wait group before stop waits on
	// it, or does not start at all.
	c.mu.Lock()
	if c.stopping {
		c.mu.Unlock()
		return
	}
	// A network fault cannot stack on another (see network): the
	// transition is recorded with the reason instead.
	if isNetworkFault(kind) && len(c.network) > 0 {
		c.events = append(c.events, &chaosEvent{Kind: kind, Trigger: trigger, Requested: time.Now(),
			Skipped: "another network fault is active; Chaos Mesh cannot stack NetworkChaos on one Pod"})
		c.mu.Unlock()
		return
	}
	d := c.reactive.next()
	// Reserved like a planner draw: it counts as active from here. A
	// reactive fault may exceed MaxConcurrent on purpose (it answers a
	// transition), but the planner then sees it and holds back.
	seq := c.reserve(kind)
	c.wg.Add(1)
	c.mu.Unlock()
	d.Kind = kind
	switch kind {
	case chaosWorkerKill, chaosWorkerFailure, chaosCPUStress, chaosMemoryStress:
		d.Scope = scopeOnePod
	case chaosCoordinatorKill, chaosCoordFailure:
		d.Scope = scopeCoordinator
	default:
		d.Scope = scopeTableGroup
	}
	go func() {
		defer c.wg.Done()
		c.inject(ctx, seq, d, trigger)
	}()
}

// networkReapplyAfter is how long a network fault may stay uninjected before
// the controller re-creates it.
const networkReapplyAfter = 15 * time.Second

// chaosRemovalTimeout bounds the removal of an experiment. Chaos Mesh holds
// the resource until it has recovered every target, and recovering a network
// fault took 3 minutes in a full-profile run (chaos-daemon retrying "unable
// to flush ip sets" on busy workers) before it succeeded; kubectlTimeout
// (2 minutes) counted that as a failed removal. An experiment still present
// after this long is one.
const chaosRemovalTimeout = 10 * time.Minute

// errNoTarget is a draw with no running Pod of the role it aims at.
var errNoTarget = errors.New("no target to aim at")

// noteManifestErr records why an experiment was not created. A draw with no
// running Pod to aim at — every worker restarting right after a coordinator
// kill — is skipped, not a failed injection: nothing was created and nothing
// failed to inject (issue #458). It reports whether the event failed.
// Caller holds c.mu for a shared event.
func noteManifestErr(ev *chaosEvent, err error) bool {
	if errors.Is(err, errNoTarget) {
		ev.Skipped = err.Error()
		return false
	}
	ev.Error = err.Error()
	return true
}

// reserved marks an active slot whose resource is not created yet.
const reserved = "reserved"

// name is the Chaos Mesh resource name of experiment seq.
func (c *chaosController) name(seq int) string {
	return fmt.Sprintf("pr-chaos-%d-%d", c.seed%100000, seq)
}

// stop ends the decision loop, waits for every experiment to end, and
// removes anything still present.
func (c *chaosController) stop() {
	c.mu.Lock()
	c.stopping = true
	c.mu.Unlock()
	close(c.stopCh)
	c.wg.Wait()
	c.mu.Lock()
	left := make(map[string]string, len(c.active))
	for name, kind := range c.active {
		left[name] = kind
	}
	c.mu.Unlock()
	for name, kind := range left {
		if kind != reserved {
			_, _ = kubectlCmdBy(time.Now().Add(chaosRemovalTimeout), "", "-n", c.ns, "delete", kind, name, "--ignore-not-found", "--wait=true")
		}
		c.forget(name)
	}
}

func (c *chaosController) forget(name string) {
	c.mu.Lock()
	delete(c.active, name)
	delete(c.network, name)
	c.mu.Unlock()
}

// inject resolves the target, applies the experiment, waits for Chaos Mesh to
// report it injected, holds it for its duration and removes it.
func (c *chaosController) inject(ctx context.Context, seq int, d chaosDecision, trigger string) {
	ev := &chaosEvent{Seq: seq, Kind: d.Kind, Scope: d.Scope, Planned: d.Duration, Requested: time.Now(), Trigger: trigger}
	ev.Name = c.name(seq)
	c.mu.Lock()
	c.events = append(c.events, ev)
	c.mu.Unlock()
	record := func(err error) {
		c.mu.Lock()
		ev.Error = err.Error()
		c.mu.Unlock()
	}

	ev.State = c.snapshot()
	manifest, resource, target, params, err := c.manifest(d, ev.Name)
	if err != nil {
		c.mu.Lock()
		noteManifestErr(ev, err)
		c.mu.Unlock()
		c.forget(ev.Name) // release the reservation: nothing was created
		return
	}
	c.mu.Lock()
	ev.Resource, ev.Target, ev.Params = resource, target, params
	c.mu.Unlock()
	var targets []podInfo
	if pods, perr := c.pods(); perr == nil {
		targets = manifestTargets(manifest, pods)
	}
	if _, err := kubectlCmd(manifest, "apply", "-f", "-"); err != nil {
		record(err)
		c.forget(ev.Name)
		return
	}
	c.mu.Lock()
	c.active[ev.Name] = resource
	c.mu.Unlock()
	defer func() {
		if _, err := kubectlCmdBy(time.Now().Add(chaosRemovalTimeout), "", "-n", c.ns, "delete", resource, ev.Name, "--ignore-not-found", "--wait=true"); err != nil {
			record(err)
		}
		c.forget(ev.Name)
		c.mu.Lock()
		ev.RemovedAt = time.Now()
		c.mu.Unlock()
	}()

	deadline := time.Now().Add(90 * time.Second)
	applied := time.Now()
	var reapplyErr error
	for {
		out, err := kubectlCmd("", "-n", c.ns, "get", resource, ev.Name, "-o",
			`jsonpath={.status.conditions[?(@.type=="AllInjected")].status}`)
		if err == nil && out == "True" {
			c.mu.Lock()
			ev.Injected, ev.InjectedAt = true, time.Now()
			c.mu.Unlock()
			break
		}
		if time.Now().After(deadline) {
			// A target that left mid-injection (recreated and Pending,
			// restarting) can never report injected: the draw lost its
			// target, skipped like a draw without one (#458).
			if now, perr := c.pods(); perr == nil {
				if why, left := targetLeft(targets, now); left {
					c.mu.Lock()
					ev.Skipped = "target left during injection: " + why
					c.mu.Unlock()
					return
				}
			}
			record(fmt.Errorf("not injected within 90s after %d re-apply(s) (last %q, %v; last re-apply error: %v)", ev.Reapplied, out, err, reapplyErr))
			return
		}
		// A network fault whose target container is restarting (the
		// coordinator exits for a clean replay when a worker dies owing
		// work) fails in chaos-daemon ("container is not running"), and
		// Chaos Mesh backs its retries off past the fault's duration (#458).
		// A fresh resource retries at once. A failed delete is retried on
		// the next poll; its error is reported if the deadline passes. Both
		// commands end by the deadline, so stop() is never held past it.
		if isNetworkFault(d.Kind) && time.Since(applied) > networkReapplyAfter {
			if _, derr := kubectlCmdBy(deadline, "", "-n", c.ns, "delete", resource, ev.Name, "--ignore-not-found", "--wait=true"); derr != nil {
				reapplyErr = derr
			} else {
				if _, aerr := kubectlCmdBy(deadline, manifest, "apply", "-f", "-"); aerr != nil {
					record(aerr)
					return
				}
				c.mu.Lock()
				ev.Reapplied++
				c.mu.Unlock()
				applied = time.Now()
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
	// pod-kill is continuous in Chaos Mesh: it kills every new Pod the
	// selector matches until the resource is deleted, so it is removed as
	// soon as the kill landed. Every other kind holds for its duration.
	if d.Kind == chaosWorkerKill || d.Kind == chaosCoordinatorKill {
		return
	}
	select {
	case <-ctx.Done():
	case <-c.stopCh:
	case <-time.After(d.Duration):
	}
}

// pods lists the pipeline's Pods with the labels the controller selects on.
func (c *chaosController) pods() ([]podInfo, error) {
	out, err := kubectlCmd("", "-n", c.ns, "get", "pods", "-l", "urutau.io/pipeline="+c.pipeline, "-o",
		`jsonpath={range .items[*]}{.metadata.name}{"\t"}{.metadata.labels.app}{"\t"}{.metadata.labels.urutau\.io/worker}{"\t"}{.status.phase}{"\t"}{.status.containerStatuses[0].restartCount}{"\t"}{.metadata.uid}{"\n"}{end}`)
	if err != nil {
		return nil, err
	}
	var pods []podInfo
	for _, line := range strings.Split(out, "\n") {
		f := strings.Split(line, "\t")
		if len(f) < 5 || f[0] == "" {
			continue
		}
		p := podInfo{name: f[0], app: f[1], group: f[2], phase: f[3], restarts: f[4]}
		if len(f) > 5 {
			p.uid = f[5]
		}
		pods = append(pods, p)
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i].name < pods[j].name })
	return pods, nil
}

type podInfo struct{ name, app, group, phase, restarts, uid string }

// manifestTargets returns the Pods of pods a manifest names in its selector.
func manifestTargets(manifest string, pods []podInfo) []podInfo {
	named := map[string]bool{}
	for _, line := range strings.Split(manifest, "\n") {
		if name, ok := strings.CutPrefix(strings.TrimSpace(line), "- "); ok {
			named[name] = true
		}
	}
	var out []podInfo
	for _, p := range pods {
		if named[p.name] {
			out = append(out, p)
		}
	}
	return out
}

// targetLeft reports whether a target of a fault left while it was being
// injected: gone, no longer Running, replaced (another UID) or restarted.
func targetLeft(targets, now []podInfo) (string, bool) {
	byName := make(map[string]podInfo, len(now))
	for _, p := range now {
		byName[p.name] = p
	}
	for _, t := range targets {
		p, ok := byName[t.name]
		switch {
		case !ok:
			return t.name + " is gone", true
		case p.phase != "Running":
			return t.name + " is " + p.phase, true
		case p.uid != t.uid:
			return t.name + " was replaced", true
		case p.restarts != t.restarts:
			return t.name + " restarted", true
		}
	}
	return "", false
}

// snapshot records the pipeline's state for an event.
func (c *chaosController) snapshot() chaosState {
	st := chaosState{Pods: map[string]string{}, WorkerReplicas: map[string]int{}}
	if c.workload != nil {
		st.Workload = c.workload()
	}
	if pods, err := c.pods(); err == nil {
		for _, p := range pods {
			st.Pods[p.name] = p.phase + " restarts=" + p.restarts
			if strings.Contains(p.name, "maint") {
				st.Maintenance++
			}
		}
	}
	if out, err := kubectlCmd("", "-n", c.ns, "get", "statefulsets", "-l", "urutau.io/pipeline="+c.pipeline, "-o",
		`jsonpath={range .items[*]}{.metadata.name}{"\t"}{.spec.replicas}{"\n"}{end}`); err == nil {
		for _, line := range strings.Split(out, "\n") {
			f := strings.Split(line, "\t")
			if len(f) == 2 {
				n, _ := strconv.Atoi(f[1])
				st.WorkerReplicas[f[0]] = n
			}
		}
	}
	c.mu.Lock()
	for name := range c.active {
		st.ActiveChaos = append(st.ActiveChaos, name)
	}
	c.mu.Unlock()
	sort.Strings(st.ActiveChaos)
	return st
}

// manifest resolves the decision's target against the current Pods and
// renders the Chaos Mesh resource.
func (c *chaosController) manifest(d chaosDecision, name string) (manifest, resource, target, params string, err error) {
	pods, err := c.pods()
	if err != nil {
		return "", "", "", "", err
	}
	return renderChaos(c.ns, c.pipeline, d, name, pods)
}

// renderChaos is manifest's pure half: target resolution and rendering
// against a given Pod list.
func renderChaos(ns, pipeline string, d chaosDecision, name string, pods []podInfo) (manifest, resource, target, params string, err error) {
	c := struct{ ns, pipeline string }{ns, pipeline}
	var workers, coordinators []podInfo
	groups := map[string]bool{}
	for _, p := range pods {
		if p.phase != "Running" {
			continue
		}
		switch p.app {
		case "urutau-coordinator":
			coordinators = append(coordinators, p)
		case "urutau-worker":
			// The coordinator's maintenance Pods live for seconds: a fault
			// aimed at one would rarely land, so they are never targets.
			if strings.HasSuffix(p.group, "-maint") {
				continue
			}
			workers = append(workers, p)
			if p.group != "" {
				groups[p.group] = true
			}
		}
	}
	pick := func(from []podInfo) (podInfo, bool) {
		if len(from) == 0 {
			return podInfo{}, false
		}
		return from[d.Pick%uint64(len(from))], true
	}
	onePod := func(p podInfo) string {
		return fmt.Sprintf("  selector:\n    pods:\n      %s:\n        - %s\n", c.ns, p.name)
	}
	coordSel := map[string]string{"app": "urutau-coordinator", "urutau.io/pipeline": c.pipeline}
	header := func(kind string) string {
		return fmt.Sprintf("apiVersion: chaos-mesh.org/v1alpha1\nkind: %s\nmetadata:\n  name: %s\n  namespace: %s\nspec:\n", kind, name, c.ns)
	}
	dur := strconv.Itoa(int(d.Duration.Seconds())) + "s"

	// podTarget resolves a one-pod or coordinator scope.
	podTarget := func() (podInfo, error) {
		var p podInfo
		var ok bool
		if d.Scope == scopeCoordinator {
			p, ok = pick(coordinators)
		} else {
			p, ok = pick(workers)
		}
		if !ok {
			return p, fmt.Errorf("%w: no running %s pod", errNoTarget, d.Scope)
		}
		return p, nil
	}

	switch d.Kind {
	case chaosWorkerKill, chaosCoordinatorKill:
		p, perr := podTarget()
		if perr != nil {
			return "", "", "", "", perr
		}
		return header("PodChaos") + "  action: pod-kill\n  mode: all\n" + onePod(p), "podchaos", p.name, "", nil
	case chaosWorkerFailure, chaosCoordFailure:
		p, perr := podTarget()
		if perr != nil {
			return "", "", "", "", perr
		}
		return header("PodChaos") + "  action: pod-failure\n  mode: all\n  duration: " + dur + "\n" + onePod(p), "podchaos", p.name, "", nil
	case chaosCPUStress, chaosMemoryStress:
		p, perr := podTarget()
		if perr != nil {
			return "", "", "", "", perr
		}
		stressor := fmt.Sprintf("    cpu:\n      workers: %d\n      load: 100\n", d.CPUWorkers)
		params = fmt.Sprintf("cpuWorkers=%d", d.CPUWorkers)
		if d.Kind == chaosMemoryStress {
			stressor = fmt.Sprintf("    memory:\n      workers: 1\n      size: \"%dMB\"\n", d.MemoryMB)
			params = fmt.Sprintf("memoryMB=%d", d.MemoryMB)
		}
		return header("StressChaos") + "  mode: all\n  duration: " + dur + "\n" + onePod(p) + "  stressors:\n" + stressor, "stresschaos", p.name, params, nil
	case chaosNetworkPartition, chaosNetworkLoss, chaosNetworkDelay:
		// The worker side is the explicit list of running Pods — every
		// worker, or one table's group — never a label selector, which
		// would also match Pods that come and go during the experiment.
		side := workers
		target = "all workers"
		if d.Scope != scopeAllWorkers && len(groups) > 0 {
			names := make([]string, 0, len(groups))
			for g := range groups {
				names = append(names, g)
			}
			sort.Strings(names)
			g := names[d.Pick%uint64(len(names))]
			side = nil
			for _, p := range workers {
				if p.group == g {
					side = append(side, p)
				}
			}
			target = g
		}
		if len(side) == 0 {
			return "", "", "", "", fmt.Errorf("%w: no running worker pod", errNoTarget)
		}
		var podList strings.Builder
		for _, p := range side {
			fmt.Fprintf(&podList, "        - %s\n", p.name)
		}
		target += " <-> coordinator"
		action := "partition"
		extra := ""
		switch d.Kind {
		case chaosNetworkLoss:
			action = "loss"
			extra = fmt.Sprintf("  loss:\n    loss: \"%d\"\n    correlation: \"25\"\n", d.LossPercent)
			params = fmt.Sprintf("loss=%d%%", d.LossPercent)
		case chaosNetworkDelay:
			action = "delay"
			extra = fmt.Sprintf("  delay:\n    latency: \"%dms\"\n    jitter: \"%dms\"\n", d.Latency.Milliseconds(), d.Latency.Milliseconds()/4)
			params = "latency=" + d.Latency.String()
		}
		body := fmt.Sprintf("  action: %s\n  mode: all\n  duration: %s\n  direction: both\n  selector:\n    pods:\n      %s:\n%s%s  target:\n    mode: all\n    selector:\n      namespaces:\n        - %s\n      labelSelectors:\n%s",
			action, dur, c.ns, podList.String(), extra, c.ns, labelSelectorYAML(coordSel, "        "))
		return header("NetworkChaos") + body, "networkchaos", target, params, nil
	}
	return "", "", "", "", fmt.Errorf("unknown chaos kind %q", d.Kind)
}

// ── report ──────────────────────────────────────────────────────────────

// chaosReport is the controller's section of the diagnostics file.
type chaosReport struct {
	Seed    uint64            `json:"seed"`
	Profile chaosProfile      `json:"profile"`
	Counts  map[chaosKind]int `json:"injectedByKind"`
	Events  []chaosEvent      `json:"events"`
}

func (c *chaosController) report() *chaosReport {
	c.mu.Lock()
	defer c.mu.Unlock()
	rep := &chaosReport{Seed: c.seed, Profile: c.profile, Counts: map[chaosKind]int{}}
	for _, ev := range c.events {
		rep.Events = append(rep.Events, *ev)
		if ev.Injected {
			rep.Counts[ev.Kind]++
		}
	}
	return rep
}

// problems reports what makes a chaos run invalid: nothing was injected, an
// experiment failed to inject or be removed, or one is still present.
func (c *chaosController) problems() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	injected := 0
	for _, ev := range c.events {
		if ev.Injected {
			injected++
		}
		if ev.Error != "" {
			out = append(out, fmt.Sprintf("chaos %s (%s → %s): %s", ev.Name, ev.Kind, ev.Target, ev.Error))
		}
	}
	if injected == 0 {
		out = append(out, "chaos: no experiment was injected")
	}
	for name := range c.active {
		out = append(out, "chaos: experiment "+name+" still present after stop")
	}
	return out
}
