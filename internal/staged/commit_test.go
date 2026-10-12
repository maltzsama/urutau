package staged

import (
	"errors"
	"testing"

	"github.com/maltzsama/urutau/position"
)

func TestTableLocksHandOutOneMutexPerTable(t *testing.T) {
	var l TableLocks
	a := l.For("a")
	if a != l.For("a") {
		t.Fatal("two mutexes for one table: its commits would not be serialized")
	}
	if a == l.For("b") {
		t.Fatal("two tables share a mutex")
	}
}

func TestMinSafePosition(t *testing.T) {
	parse := func(s string) (position.Position, error) {
		if s == "x" {
			return nil, errors.New("unparsable")
		}
		return position.MustLSN(s), nil
	}
	if got, err := MinSafePosition(parse, []string{"0/20", "", "0/10"}); err != nil || got != "0/10" {
		t.Fatalf("MinSafePosition = %q, %v; want 0/10", got, err)
	}
	if got, err := MinSafePosition(parse, nil); err != nil || got != "" {
		t.Fatalf("MinSafePosition(none) = %q, %v; want empty", got, err)
	}
	if _, err := MinSafePosition(parse, []string{"0/10", "x"}); err == nil {
		t.Fatal("an unparsable position was accepted")
	}
}

func TestCycleCommitPosition(t *testing.T) {
	parse := func(s string) (position.Position, error) { return position.MustLSN(s), nil }
	positions := []string{"0/30", "0/20"}

	window := &Cycle{Window: true, BatchPos: "0/40", Positions: positions}
	if got, err := window.CommitPosition(parse); err != nil || got != "" {
		t.Fatalf("window cycle commits %q, %v; want no position", got, err)
	}
	batch := &Cycle{BatchPos: "0/40", Positions: positions}
	if got, err := batch.CommitPosition(parse); err != nil || got != "0/40" {
		t.Fatalf("one-batch cycle commits %q, %v; want the batch's last position", got, err)
	}
	other := &Cycle{Positions: positions}
	if got, err := other.CommitPosition(parse); err != nil || got != "0/20" {
		t.Fatalf("cycle commits %q, %v; want the minimum over its deliveries", got, err)
	}
}
