package spec

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrNilFilterNode       = errors.New("filter node is empty")
	ErrAmbiguousFilterNode = errors.New("filter node carries more than one of all/any/not/where")
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
	}

	if len(problems) > 0 {
		return fmt.Errorf("spec: %s", strings.Join(problems, "; "))
	}
	return nil
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
