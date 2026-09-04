package worker

import (
	"testing"

	"github.com/maltzsama/urutau/internal/position"
)

// Each source kind must parse its own position format back. Kafka is the
// case that broke: positions are offsets, and parsing them as GTID sets
// failed the worker boot the moment a committed position existed.
func TestParsePositionPerKind(t *testing.T) {
	pg, err := parsePosition("postgres")("0/1A")
	if err != nil {
		t.Fatalf("postgres: %v", err)
	}
	if pg.(*position.LSN).String() != "0/1A" {
		t.Fatalf("postgres parsed %v", pg)
	}

	my, err := parsePosition("mysql")("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	if err != nil {
		t.Fatalf("mysql: %v", err)
	}
	if _, ok := my.(*position.GTID); !ok {
		t.Fatalf("mysql parsed %T, want *position.GTID", my)
	}

	off := &position.Offsets{Topic: "shop.orders", Parts: map[int32]int64{0: 42}}
	kf, err := parsePosition("kafka")(off.String())
	if err != nil {
		t.Fatalf("kafka: %v", err)
	}
	got, ok := kf.(*position.Offsets)
	if !ok {
		t.Fatalf("kafka parsed %T, want *position.Offsets", kf)
	}
	if got.Topic != "shop.orders" || got.Parts[0] != 42 {
		t.Fatalf("kafka parsed %+v, want topic shop.orders partition 0 @42", got)
	}
}
