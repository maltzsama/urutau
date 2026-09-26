package pods

// Cluster-free tests of the production-readiness gates.

import (
	"strings"
	"testing"
	"time"
)

func TestRegressed(t *testing.T) {
	const u = "4acc018a-b8ce-11f1-a7c2-be924e253b21:"
	cases := []struct {
		prev, next string
		want       bool
	}{
		{u + "1-10", u + "1-12", false},
		{u + "1-12", u + "1-10", true},
		{u + "1-10", u + "1-10", false},
		{"", u + "1-10", false},
		{u + "1-10", "", false},
		{u + "1-10", "garbage", false},
	}
	for _, c := range cases {
		if got := regressed(c.prev, c.next); got != c.want {
			t.Errorf("regressed(%q, %q) = %v, want %v", c.prev, c.next, got, c.want)
		}
	}
}

func samplerWith(samples map[string][]progressSample) *progressSampler {
	s := newProgressSampler(nil, nil, time.Second, 5*time.Minute)
	s.samples = samples
	return s
}

// A table stuck while its source changes and another table advances is
// starved; every table stuck at once (a restart) is not.
func TestCheckStarvation(t *testing.T) {
	t0 := time.Now().Add(-10 * time.Minute)
	at := func(m int) time.Time { return t0.Add(time.Duration(m) * time.Minute) }

	starved := samplerWith(map[string][]progressSample{
		"a": {{At: at(0), Produced: 10, Committed: "p1"}, {At: at(6), Produced: 90, Committed: "p1"}},
		"b": {{At: at(0), Produced: 10, Committed: "q1"}, {At: at(6), Produced: 90, Committed: "q9"}},
	})
	starved.checkStarvation(at(6))
	if _, p := starved.report(); len(p) != 1 || !strings.Contains(p[0], "a: starved") {
		t.Fatalf("problems = %v, want one naming a as starved", p)
	}

	paused := samplerWith(map[string][]progressSample{
		"a": {{At: at(0), Produced: 10, Committed: "p1"}, {At: at(6), Produced: 90, Committed: "p1"}},
		"b": {{At: at(0), Produced: 10, Committed: "q1"}, {At: at(6), Produced: 90, Committed: "q1"}},
	})
	paused.checkStarvation(at(6))
	if _, p := paused.report(); len(p) != 0 {
		t.Fatalf("every table paused together is not starvation, got %v", p)
	}

	idle := samplerWith(map[string][]progressSample{
		"a": {{At: at(0), Produced: 10, Committed: "p1"}, {At: at(6), Produced: 10, Committed: "p1"}},
		"b": {{At: at(0), Produced: 10, Committed: "q1"}, {At: at(6), Produced: 90, Committed: "q9"}},
	})
	idle.checkStarvation(at(6))
	if _, p := idle.report(); len(p) != 0 {
		t.Fatalf("a table whose source did not change is not starved, got %v", p)
	}

	short := samplerWith(map[string][]progressSample{
		"a": {{At: at(3), Produced: 10, Committed: "p1"}, {At: at(6), Produced: 90, Committed: "p1"}},
		"b": {{At: at(3), Produced: 10, Committed: "q1"}, {At: at(6), Produced: 90, Committed: "q9"}},
	})
	short.checkStarvation(at(6))
	if _, p := short.report(); len(p) != 0 {
		t.Fatalf("a history shorter than the window cannot starve, got %v", p)
	}
}
