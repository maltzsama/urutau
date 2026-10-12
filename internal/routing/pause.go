package routing

import "sync"

// Pause holds back the input of a table whose re-slice is draining, so the
// drain can converge: a continuously loaded table never owes nothing on its
// own. The router buffers a paused table's batches (Hold) instead of parking
// on them — parking would stall every other table behind the paused one —
// and routes them, in order and by the new layout, once the table resumes
// (TakeResumed).
//
// B is the caller's batch. Hold and TakeResumed must be called by the one
// goroutine that routes: nothing may be routed between a held batch and the
// batches that follow it. The zero value is ready to use.
type Pause[B any] struct {
	mu     sync.Mutex
	paused map[string]struct{}
	held   map[string][]B
	wake   chan struct{}
}

// init allocates the state on first use. Callers hold mu.
func (p *Pause[B]) init() {
	if p.wake == nil {
		p.paused = map[string]struct{}{}
		p.held = map[string][]B{}
		p.wake = make(chan struct{}, 1)
	}
}

// Pause marks target paused. Pausing a paused table changes nothing.
func (p *Pause[B]) Pause(target string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.init()
	p.paused[target] = struct{}{}
}

// Resume releases target and wakes the router to route what it held.
func (p *Pause[B]) Resume(target string) {
	p.mu.Lock()
	p.init()
	delete(p.paused, target)
	wake := p.wake
	p.mu.Unlock()
	select {
	case wake <- struct{}{}:
	default: // a wake is already pending; TakeResumed returns every resumed table
	}
}

// Paused reports whether target is paused.
func (p *Pause[B]) Paused(target string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.paused[target]
	return ok
}

// Wake returns the channel that receives after a Resume.
func (p *Pause[B]) Wake() <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.init()
	return p.wake
}

// Hold reports whether b was buffered for target rather than left for the
// caller to route. A table that is paused, or that still has held batches
// waiting to be routed, buffers: the second condition keeps the order when a
// batch for the resumed table arrives before the router handles the wake.
func (p *Pause[B]) Hold(target string, b B) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.init()
	if _, paused := p.paused[target]; !paused && len(p.held[target]) == 0 {
		return false
	}
	p.held[target] = append(p.held[target], b)
	return true
}

// TakeResumed returns the batches held for every table no longer paused, in
// arrival order, and forgets them: they are the caller's to route or
// release. A table still paused keeps its batches.
func (p *Pause[B]) TakeResumed() map[string][]B {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := map[string][]B{}
	for target, buf := range p.held {
		if len(buf) == 0 {
			continue
		}
		if _, paused := p.paused[target]; paused {
			continue
		}
		out[target] = buf
		delete(p.held, target)
	}
	return out
}

// Held returns how many batches are held for target.
func (p *Pause[B]) Held(target string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.held[target])
}

// HeldTables returns the tables with held batches, in no particular order.
func (p *Pause[B]) HeldTables() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for target, buf := range p.held {
		if len(buf) > 0 {
			out = append(out, target)
		}
	}
	return out
}
