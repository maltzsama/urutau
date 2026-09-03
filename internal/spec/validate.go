package spec

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/maltzsama/urutau/internal/core"
)

var (
	ErrNilFilterNode       = errors.New("filter node is empty")
	ErrAmbiguousFilterNode = errors.New("filter node carries more than one of all/any/not/where")
	// identRe is the closed set of valid destination column identifiers.
	identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// Validate applies the hard, server-side rules. The same code must validate
// inline specs and resolved specs — validation is single and server-side by
// design.
func (s *Spec) Validate() error {
	var problems []string

	if s.Pipeline == "" {
		problems = append(problems, "pipeline: required")
	}

	switch s.Source.Kind {
	case "mysql", "postgres":
	case "":
		problems = append(problems, "source.kind: required (mysql | postgres)")
	default:
		problems = append(problems, fmt.Sprintf("source.kind: unsupported %q", s.Source.Kind))
	}
	if s.Source.Kind == "postgres" && s.Source.SlotName == "" {
		problems = append(problems, "source.slotName: required for postgres (logical replication slot)")
	}
	if s.Source.URI == "" {
		problems = append(problems, "source.uri: required")
	}

	if s.Sink.URI == "" {
		problems = append(problems, "sink.uri: required")
	}
	if s.Sink.Namespace == "" {
		problems = append(problems, "sink.namespace: required")
	}

	if len(s.Tables) == 0 {
		problems = append(problems, "tables: at least one required")
	}

	seenSource := map[string]bool{}
	seenTarget := map[string]bool{}
	for i, tbl := range s.Tables {
		p := fmt.Sprintf("tables[%d]", i)
		if tbl.Source == "" {
			problems = append(problems, p+".source: required")
		}
		if tbl.Target == "" {
			problems = append(problems, p+".target: required")
		}
		if tbl.Source != "" {
			if seenSource[tbl.Source] {
				problems = append(problems, fmt.Sprintf("%s.source: duplicated %q", p, tbl.Source))
			}
			seenSource[tbl.Source] = true
		}
		if tbl.Target != "" {
			if seenTarget[tbl.Target] {
				problems = append(problems, fmt.Sprintf("%s.target: duplicated %q", p, tbl.Target))
			}
			seenTarget[tbl.Target] = true
		}

		mode := tbl.WriteMode
		if mode == "" {
			mode = s.Sink.Defaults.WriteMode
		}
		if mode == "" {
			mode = WriteModeUpsert // upsert-first: reflecting state is the default
		}
		switch mode {
		case WriteModeUpsert:
			if len(tbl.PrimaryKey) == 0 {
				problems = append(problems, p+".primaryKey: required when writeMode is upsert")
			}
		case WriteModeAppend:
			if tbl.Filter != nil && !tbl.FilterImmutable {
				problems = append(problems, p+".filterImmutable: required when writeMode is append and a filter is set")
			}
		default:
			problems = append(problems, fmt.Sprintf("%s.writeMode: unsupported %q", p, mode))
		}

		validateFilter(tbl.Filter, p+".filter", &problems)
		validateMetadata(tbl, p, &problems)
		validateCast(tbl, p, &problems)
	}

	if len(problems) > 0 {
		return fmt.Errorf("spec: %s", strings.Join(problems, "; "))
	}
	return nil
}

// validMetadataKeys is the closed metadata catalog.
var validMetadataKeys = map[core.MetadataKey]bool{
	core.MetaOp:          true,
	core.MetaCommitTS:    true,
	core.MetaIngestTS:    true,
	core.MetaPosition:    true,
	core.MetaSourceTable: true,
	core.MetaPhase:       true,
}

// validateMetadata checks the closed metadata rules: catalog membership,
// explicit valid destination name, no repeats, never part of the primary
// key.
func validateMetadata(tbl Table, path string, problems *[]string) {
	seen := map[string]bool{}
	for _, m := range tbl.Metadata {
		if !validMetadataKeys[m.From] {
			*problems = append(*problems, fmt.Sprintf("%s.metadata.from: unknown key %q (catalog: op, commit_ts, ingest_ts, position, source_table, phase)", path, m.From))
		}
		if m.As == "" {
			*problems = append(*problems, fmt.Sprintf("%s.metadata.as: required", path))
		} else if !identRe.MatchString(m.As) {
			*problems = append(*problems, fmt.Sprintf("%s.metadata.as: %q is not a valid column name", path, m.As))
		}
		if m.As != "" && seen[m.As] {
			*problems = append(*problems, fmt.Sprintf("%s.metadata.as: duplicated %q", path, m.As))
		}
		seen[m.As] = true
		if m.As != "" {
			for _, pk := range tbl.PrimaryKey {
				if pk == m.As {
					*problems = append(*problems, fmt.Sprintf("%s.metadata: column %q cannot be part of the primary key (the equality key comes from the source)", path, m.As))
				}
			}
		}
	}
}

// validateCast checks the closed cast rules that are decidable without
// source introspection: every value must parse to a canonical target, and
// the key must not be empty. The matrix (source kind → target) is enforced
// at schema resolution, where the source type is known.
func validateCast(tbl Table, path string, problems *[]string) {
	for name, text := range tbl.Cast {
		if name == "" {
			*problems = append(*problems, fmt.Sprintf("%s.cast: empty column name", path))
			continue
		}
		if _, err := core.ParseCastTarget(text); err != nil {
			*problems = append(*problems, fmt.Sprintf("%s.cast.%s: %v", path, name, err))
		}
	}
}

// Warnings returns the advisory outcomes of the spec — rules that do not
// reject it but must be surfaced to the operator (eventlog, status).
func (s *Spec) Warnings() []string {
	var warns []string
	for i, tbl := range s.Tables {
		mode := tbl.WriteMode
		if mode == "" {
			mode = s.Sink.Defaults.WriteMode
		}
		if mode == "" {
			mode = WriteModeUpsert
		}
		if mode == WriteModeUpsert {
			for _, m := range tbl.Metadata {
				if m.From == core.MetaOp {
					warns = append(warns, fmt.Sprintf("tables[%d].metadata.op: in upsert mode a delete removes the row and op never lands as \"delete\" — use writeMode append to keep deletes", i))
				}
			}
		}
	}
	return warns
}

// validateFilter checks the closed grammar: every node carries exactly one
// of all/any/not/where, and predicates only use known operators with values
// of the right shape.
func validateFilter(f *Filter, path string, problems *[]string) {
	if f == nil {
		return
	}

	count := 0
	if len(f.All) > 0 {
		count++
		for i := range f.All {
			validateFilter(&f.All[i], path+".all["+fmt.Sprint(i)+"]", problems)
		}
	}
	if len(f.Any) > 0 {
		count++
		for i := range f.Any {
			validateFilter(&f.Any[i], path+".any["+fmt.Sprint(i)+"]", problems)
		}
	}
	if f.Not != nil {
		count++
		validateFilter(f.Not, path+".not", problems)
	}
	if f.Predicate != nil {
		count++
		validatePredicate(f.Predicate, path+".where", problems)
	}

	if count == 0 {
		*problems = append(*problems, fmt.Sprintf("%s: %v", path, ErrNilFilterNode))
	}
	if count > 1 {
		*problems = append(*problems, fmt.Sprintf("%s: %v", path, ErrAmbiguousFilterNode))
	}
}

func validatePredicate(p *Predicate, path string, problems *[]string) {
	if p.Column == "" {
		*problems = append(*problems, path+".col: required")
	}

	switch p.Op {
	case OpEq, OpNeq, OpLt, OpLte, OpGt, OpGte:
		if p.Value == nil {
			*problems = append(*problems, fmt.Sprintf("%s: op %q requires a value", path, p.Op))
		}
	case OpIn, OpNotIn:
		if _, ok := p.Value.([]any); !ok {
			*problems = append(*problems, fmt.Sprintf("%s: op %q requires a list value", path, p.Op))
		}
	case OpIsNull, OpIsNotNull:
		if p.Value != nil {
			*problems = append(*problems, fmt.Sprintf("%s: op %q takes no value", path, p.Op))
		}
	case "":
		*problems = append(*problems, path+".op: required")
	default:
		*problems = append(*problems, fmt.Sprintf("%s.op: unknown %q", path, p.Op))
	}
}
