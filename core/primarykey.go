package core

import "fmt"

// rejectFloatPrimaryKey rejects a schema whose primary key contains a
// floating-point column. Equality on floats is ill-defined (NaN, -0.0, and
// rounding across engines), and Iceberg forbids float/double as identifier
// fields and equality-delete keys. The rule is a property of the key, not of
// one sink, so it lives here: every sink in upsert mode fails at boot, before
// any table is created or any row is read (issue #515).
func rejectFloatPrimaryKey(s Schema) error {
	for _, name := range s.PrimaryKey {
		col, ok := s.Column(name)
		if !ok {
			continue // Schema.Validate/KeyIndexes reports a missing key column
		}
		if col.Type.Kind == KindFloat32 || col.Type.Kind == KindFloat64 {
			return fmt.Errorf("core: primary key column %q is %s — floating-point keys are not allowed (equality on floats is ill-defined: NaN, -0.0, rounding); use an integer, string, decimal, uuid or timestamp key", name, col.Type.Kind)
		}
	}
	return nil
}
