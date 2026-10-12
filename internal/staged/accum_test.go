package staged

import (
	"testing"
	"time"
)

func TestNewBoundsTakesTheDefaultOfAnUnsetLimit(t *testing.T) {
	b := NewBounds(0, 0, 0)
	if b.MaxRows != DefaultCycleMaxRows || b.MaxBytes != DefaultCycleMaxBytes || b.MaxAge != DefaultCycleMaxAge {
		t.Fatalf("bounds = %+v, want the defaults", b)
	}
	b = NewBounds(7, 9, time.Minute)
	if b.MaxRows != 7 || b.MaxBytes != 9 || b.MaxAge != time.Minute {
		t.Fatalf("bounds = %+v, want the configured limits", b)
	}
}

// The first batch always fits, so one oversized batch is still sent; the
// next ones fit only within both bounds.
func TestBoundsFits(t *testing.T) {
	b := Bounds{MaxRows: 10, MaxBytes: 100}
	if !b.Fits(0, 0, 50, 5000) {
		t.Fatal("the first batch did not fit")
	}
	if !b.Fits(4, 40, 6, 60) {
		t.Fatal("a batch reaching both bounds exactly did not fit")
	}
	if b.Fits(4, 40, 7, 10) {
		t.Fatal("a batch past the row bound fit")
	}
	if b.Fits(4, 40, 1, 61) {
		t.Fatal("a batch past the byte bound fit")
	}
}

func TestAccumFullAtAnyBound(t *testing.T) {
	now := time.Now()
	b := Bounds{MaxRows: 10, MaxBytes: 100, MaxAge: time.Second}
	var none *Accum
	if none.Holds() {
		t.Fatal("a nil accumulator holds a batch")
	}
	a := NewAccum(now)
	if a.Holds() {
		t.Fatal("an empty accumulator holds a batch")
	}
	a.Add(nil, 3, 30)
	if !a.Holds() || a.Rows != 3 || a.Bytes != 30 {
		t.Fatalf("accumulator = %+v, want one batch of 3 rows and 30 bytes", a)
	}
	if a.Full(b, now) {
		t.Fatal("full below every bound")
	}
	if !a.Full(b, now.Add(time.Second)) {
		t.Fatal("not full at the age bound")
	}
	a.Add(nil, 7, 1)
	if !a.Full(b, now) {
		t.Fatal("not full at the row bound")
	}
	a = NewAccum(now)
	a.Add(nil, 1, 100)
	if !a.Full(b, now) {
		t.Fatal("not full at the byte bound")
	}
}
