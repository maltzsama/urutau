package position

import "testing"

func TestLSNParseAndString(t *testing.T) {
	cases := []string{"0/0", "0/1A2B3C", "7/D80B2C08", "FFFFFFFF/FFFFFFFF"}
	for _, raw := range cases {
		l, err := ParseLSN(raw)
		if err != nil {
			t.Fatalf("parse %q: %v", raw, err)
		}
		if l.String() != raw {
			t.Errorf("round trip: %q → %q", raw, l.String())
		}
	}

	bad := []string{"", "nodigit", "1-2", "zz/yy", "0/", "/123", "1/2/3"}
	for _, raw := range bad {
		if _, err := ParseLSN(raw); err == nil {
			t.Errorf("parse %q: want error", raw)
		}
	}
}

func TestLSNCompareAndContains(t *testing.T) {
	lo := MustLSN("0/10")
	hi := MustLSN("0/20")
	eq := MustLSN("0/10")

	if lo.Compare(hi) >= 0 {
		t.Error("0/10 must be less than 0/20")
	}
	if hi.Compare(lo) <= 0 {
		t.Error("0/20 must be greater than 0/10")
	}
	if lo.Compare(eq) != 0 {
		t.Error("equal LSNs must compare equal")
	}
	if !hi.Contains(lo) || !lo.Contains(lo) {
		t.Error("containment is at-or-before")
	}
	if lo.Contains(hi) {
		t.Error("0/10 must not contain 0/20")
	}

	// High/low split: 1/0 is greater than any low part under 0.
	hiHalf := MustLSN("1/0")
	if hiHalf.Compare(MustLSN("0/FFFFFFFF")) <= 0 {
		t.Error("1/0 must exceed 0/FFFFFFFF")
	}
}

func TestLSNCompareRejectsForeignType(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("comparing LSN to GTID must panic (type mix is a bug)")
		}
	}()
	MustLSN("0/1").Compare(MustGTID(""))
}

func TestMinLSNs(t *testing.T) {
	got := Min([]Position{MustLSN("0/30"), MustLSN("0/10"), MustLSN("0/20")})
	if got.String() != "0/10" {
		t.Errorf("Min = %s, want 0/10", got)
	}
	if Min(nil) != nil {
		t.Error("Min of nothing is nil")
	}
}
