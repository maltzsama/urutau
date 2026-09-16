package spec

import (
	"strings"
	"testing"
)

func TestParseBytes(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"512", 512},
		{"0", 0},
		{"1Ki", 1024},
		{"128Mi", 128 * 1024 * 1024},
		{"512Mi", 512 * 1024 * 1024},
		{"1Gi", 1024 * 1024 * 1024},
		{"2Ti", 2 * 1024 * 1024 * 1024 * 1024},
	}
	for _, c := range cases {
		got, err := ParseBytes(c.in)
		if err != nil {
			t.Errorf("ParseBytes(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseBytes(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

// "1K" (no "i") must not be silently accepted as if it meant 1Ki — Ki/Mi/Gi/Ti
// is the only spelling this parser understands, matching the existing
// targetFileSize convention (spec/load_test.go's "128Mi" fixture).
func TestParseBytesRejectsNonBinaryUnit(t *testing.T) {
	for _, bad := range []string{
		"1K", "1M", "1G", "1.5Mi", "-1Mi", "Mi", "", "abc",
		// Multiplication overflow must be a loud error, not a wrapped
		// (negative) byte count.
		"9223372036854775807Mi", "9007199254740992Gi", "9223372036854775807Ti",
	} {
		if _, err := ParseBytes(bad); err == nil {
			t.Errorf("ParseBytes(%q): want error, got none", bad)
		}
	}
}

func maintSpec() *Spec {
	s := validSpec()
	s.Sink.Maintenance = &Maintenance{
		Enabled: true,
		Compaction: &CompactionConfig{
			Interval:       "5m",
			TargetFileSize: "512Mi",
			MinInputFiles:  5,
		},
		SnapshotExpiry: &SnapshotExpiryConfig{
			Interval:   "10m",
			RetainLast: 1,
			MaxAge:     "168h",
		},
		OrphanCleanup: &OrphanCleanupConfig{
			Interval:  "1h",
			OlderThan: "72h",
		},
	}
	return s
}

func TestValidateMaintenanceAccepted(t *testing.T) {
	if err := maintSpec().Validate(); err != nil {
		t.Fatalf("well-formed maintenance config must validate: %v", err)
	}
}

// Maintenance is an Iceberg-only feature. Configuring it on another sink
// type must be a validation error, not a block that validates and then
// silently never runs — the operator wrote a feature the sink cannot honor.
func TestValidateMaintenanceRejectsNonIcebergSink(t *testing.T) {
	for _, typ := range []string{"clickhouse", "couchbase"} {
		s := maintSpec()
		s.Sink.Type = typ
		err := s.Validate()
		if err == nil {
			t.Errorf("sink.type %q: maintenance must be rejected, got no error", typ)
			continue
		}
		if !strings.Contains(err.Error(), "sink.maintenance") {
			t.Errorf("sink.type %q: want a sink.maintenance problem, got %v", typ, err)
		}
	}
}

// The Iceberg family is the supported set: the default (empty, normalized
// to "iceberg+rest") and any future "iceberg+<catalog>" variant.
func TestValidateMaintenanceAcceptsIcebergSink(t *testing.T) {
	for _, typ := range []string{"", "iceberg", "iceberg+rest", "iceberg+glue"} {
		s := maintSpec()
		s.Sink.Type = typ
		if err := s.Validate(); err != nil {
			t.Errorf("sink.type %q: maintenance must validate: %v", typ, err)
		}
	}
}

// nil Maintenance, and nil sub-configs within a non-nil Maintenance, must not
// be treated as "empty struct with invalid zero values" — they mean
// "this operation is disabled," not "misconfigured."
func TestValidateMaintenanceNilIsFine(t *testing.T) {
	s := validSpec()
	if err := s.Validate(); err != nil {
		t.Fatalf("nil maintenance must validate: %v", err)
	}
	s.Sink.Maintenance = &Maintenance{Enabled: true}
	if err := s.Validate(); err != nil {
		t.Fatalf("maintenance with all-nil sub-configs must validate: %v", err)
	}
}

func TestValidateMaintenanceBadDurations(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Spec)
		want   string
	}{
		{"compaction interval", func(s *Spec) { s.Sink.Maintenance.Compaction.Interval = "five minutes" }, "compaction.interval"},
		{"compaction targetFileSize", func(s *Spec) { s.Sink.Maintenance.Compaction.TargetFileSize = "big" }, "compaction.targetFileSize"},
		{"snapshotExpiry interval", func(s *Spec) { s.Sink.Maintenance.SnapshotExpiry.Interval = "soon" }, "snapshotExpiry.interval"},
		{"snapshotExpiry maxAge", func(s *Spec) { s.Sink.Maintenance.SnapshotExpiry.MaxAge = "a week" }, "snapshotExpiry.maxAge"},
		{"snapshotExpiry retainLast", func(s *Spec) { s.Sink.Maintenance.SnapshotExpiry.RetainLast = -1 }, "snapshotExpiry.retainLast"},
		{"orphanCleanup interval", func(s *Spec) { s.Sink.Maintenance.OrphanCleanup.Interval = "hourly" }, "orphanCleanup.interval"},
		{"orphanCleanup olderThan", func(s *Spec) { s.Sink.Maintenance.OrphanCleanup.OlderThan = "old" }, "orphanCleanup.olderThan"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := maintSpec()
			c.mutate(s)
			err := s.Validate()
			if err == nil {
				t.Fatalf("want a problem containing %q, got no error", c.want)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("want a problem containing %q, got %v", c.want, err)
			}
		})
	}
}

func TestValidateTargetFileSize(t *testing.T) {
	s := validSpec()
	s.Sink.Defaults.TargetFileSize = "not-a-size"
	err := s.Validate()
	if err == nil || !strings.Contains(err.Error(), "targetFileSize") {
		t.Fatalf("want targetFileSize problem, got %v", err)
	}
}

// Maintenance defaults to disabled — an operator who never writes a
// maintenance: block must get NO background compaction/expiry/cleanup, not
// an implicit opt-in. sampleYAML (spec/load_test.go) never mentions
// maintenance, so this is the exact document an ordinary pipeline spec
// looks like today. MaintenanceEnabled() is what runner.go/coordinator.go
// actually call to decide whether to launch the Maintainer goroutine, so
// the test asserts against that method, not the raw field.
func TestLoadYAMLWithoutMaintenanceBlockIsDisabled(t *testing.T) {
	s, err := LoadYAML(strings.NewReader(sampleYAML))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if s.Sink.Maintenance != nil {
		t.Fatalf("Sink.Maintenance = %+v, want nil — a spec that never mentions maintenance must not enable it", s.Sink.Maintenance)
	}
	if s.Sink.MaintenanceEnabled() {
		t.Fatal("MaintenanceEnabled() must be false when the spec never declared a maintenance block")
	}
}

// MaintenanceEnabled must stay false for every way "not explicitly turned
// on" can be spelled: no block at all, an empty block (Enabled left at its
// bool zero value), and a fully-populated block whose operator forgot
// enabled: true. Only an explicit Enabled: true flips it.
func TestMaintenanceEnabledCases(t *testing.T) {
	cases := []struct {
		name string
		sink Sink
		want bool
	}{
		{"nil block", Sink{}, false},
		{"empty block", Sink{Maintenance: &Maintenance{}}, false},
		{"populated but not enabled", Sink{Maintenance: &Maintenance{
			Compaction: &CompactionConfig{Interval: "5m"},
		}}, false},
		{"explicitly enabled", Sink{Maintenance: &Maintenance{Enabled: true}}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.sink.MaintenanceEnabled(); got != c.want {
				t.Errorf("MaintenanceEnabled() = %v, want %v", got, c.want)
			}
		})
	}
}
