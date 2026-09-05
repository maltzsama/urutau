package position

import (
	"testing"
)

func TestParseOffsets(t *testing.T) {
	o, err := ParseOffsets("orders:p0=10,p1=20")
	if err != nil {
		t.Fatal(err)
	}
	if o.Topic != "orders" {
		t.Errorf("topic = %q, want orders", o.Topic)
	}
	if o.Parts[0] != 10 || o.Parts[1] != 20 {
		t.Errorf("parts = %v, want {0:10, 1:20}", o.Parts)
	}
}

func TestParseOffsetsEmpty(t *testing.T) {
	o, err := ParseOffsets("orders:")
	if err != nil {
		t.Fatal(err)
	}
	if len(o.Parts) != 0 {
		t.Errorf("parts = %v, want empty", o.Parts)
	}
}

func TestParseOffsetsBadFormat(t *testing.T) {
	if _, err := ParseOffsets("no-colon"); err == nil {
		t.Error("expected error for missing colon")
	}
	if _, err := ParseOffsets("t:bad"); err == nil {
		t.Error("expected error for bad pair")
	}
}

func TestOffsetsString(t *testing.T) {
	o := &Offsets{Topic: "orders", Parts: map[int32]int64{1: 20, 0: 10}}
	s := o.String()
	if s != "orders:p0=10,p1=20" {
		t.Errorf("String() = %q, want orders:p0=10,p1=20", s)
	}
}

func TestOffsetsCompare(t *testing.T) {
	a := &Offsets{Topic: "t", Parts: map[int32]int64{0: 10}}
	b := &Offsets{Topic: "t", Parts: map[int32]int64{0: 20}}
	if a.Compare(b) >= 0 {
		t.Error("a should be less than b")
	}
	if b.Compare(a) <= 0 {
		t.Error("b should be greater than a")
	}
}

func TestOffsetsCompareDifferentTopic(t *testing.T) {
	a := &Offsets{Topic: "a", Parts: map[int32]int64{0: 100}}
	b := &Offsets{Topic: "b", Parts: map[int32]int64{0: 1}}
	if a.Compare(b) >= 0 {
		t.Error("topic 'a' should sort before 'b'")
	}
}

func TestOffsetsContains(t *testing.T) {
	a := &Offsets{Topic: "t", Parts: map[int32]int64{0: 10, 1: 20}}
	b := &Offsets{Topic: "t", Parts: map[int32]int64{0: 5, 1: 15}}
	if !a.Contains(b) {
		t.Error("a should contain b")
	}
	if b.Contains(a) {
		t.Error("b should not contain a")
	}
}

func TestOffsetsContainsExtraPartition(t *testing.T) {
	a := &Offsets{Topic: "t", Parts: map[int32]int64{0: 10, 1: 20, 2: 30}}
	b := &Offsets{Topic: "t", Parts: map[int32]int64{0: 5, 1: 15}}
	if !a.Contains(b) {
		t.Error("a with extra partition should contain b")
	}
}

func TestOffsetsContainsDifferentTopic(t *testing.T) {
	a := &Offsets{Topic: "a", Parts: map[int32]int64{0: 10}}
	b := &Offsets{Topic: "b", Parts: map[int32]int64{0: 5}}
	if a.Contains(b) {
		t.Error("different topics should not contain")
	}
}

func TestOffsetsJSONRoundTrip(t *testing.T) {
	original := &Offsets{Topic: "orders", Parts: map[int32]int64{0: 10, 2: 30}}
	data, err := original.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var decoded Offsets
	if err := decoded.UnmarshalJSON(data); err != nil {
		t.Fatal(err)
	}
	if decoded.Topic != original.Topic {
		t.Errorf("topic = %q, want %q", decoded.Topic, original.Topic)
	}
	if len(decoded.Parts) != len(original.Parts) {
		t.Errorf("parts len = %d, want %d", len(decoded.Parts), len(original.Parts))
	}
	for p, off := range original.Parts {
		if decoded.Parts[p] != off {
			t.Errorf("part %d offset = %d, want %d", p, decoded.Parts[p], off)
		}
	}
}
