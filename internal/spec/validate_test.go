package spec

import (
	"strings"
	"testing"
)

func validSpec() *Spec {
	return &Spec{
		Pipeline: "ibe",
		Source:   Source{Kind: "mysql", URI: "mysql://user@localhost:3306/db"},
		Sink:     Sink{URI: "polaris://localhost:8181/api/catalog", Namespace: "raw"},
		Tables: []Table{
			{Source: "ibe.orders", Target: "raw.orders", PrimaryKey: []string{"id"}},
		},
	}
}

func TestValidateAcceptsMinimalUpsert(t *testing.T) {
	if err := validSpec().Validate(); err != nil {
		t.Fatalf("minimal spec must validate: %v", err)
	}
}

func TestValidateUpsertRequiresPrimaryKey(t *testing.T) {
	s := validSpec()
	s.Tables[0].PrimaryKey = nil
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "primaryKey") {
		t.Fatalf("want primaryKey problem, got %v", err)
	}
}

func TestValidateAppendWithFilterRequiresImmutable(t *testing.T) {
	s := validSpec()
	s.Tables[0].WriteMode = WriteModeAppend
	s.Tables[0].PrimaryKey = nil
	s.Tables[0].Filter = &Filter{Predicate: &Predicate{Column: "status", Op: OpNeq, Value: "draft"}}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "filterImmutable") {
		t.Fatalf("want filterImmutable problem, got %v", err)
	}

	s.Tables[0].FilterImmutable = true
	if err := s.Validate(); err != nil {
		t.Fatalf("append with immutable filter must validate: %v", err)
	}
}

func TestValidateUnsupportedWriteMode(t *testing.T) {
	s := validSpec()
	s.Tables[0].WriteMode = WriteMode("merge")
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "writeMode") {
		t.Fatalf("want writeMode problem, got %v", err)
	}
}

func TestValidateDefaultsToUpsert(t *testing.T) {
	s := validSpec()
	s.Tables[0].WriteMode = ""
	if err := s.Validate(); err != nil {
		t.Fatalf("empty writeMode must fall back to upsert default: %v", err)
	}

	s.Tables[0].PrimaryKey = nil
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "primaryKey") {
		t.Fatalf("default upsert must still demand a primary key: %v", err)
	}
}

func TestValidateDuplicateTargets(t *testing.T) {
	s := validSpec()
	s.Tables = append(s.Tables, Table{Source: "ibe.other", Target: "raw.orders", PrimaryKey: []string{"id"}})
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("want duplication problem, got %v", err)
	}
}

func TestValidateFilterGrammar(t *testing.T) {
	cases := []struct {
		name    string
		filter  *Filter
		wantErr string
	}{
		{"empty node", &Filter{}, "empty"},
		{"ambiguous node", &Filter{
			Predicate: &Predicate{Column: "a", Op: OpEq, Value: 1},
			Not:       &Filter{Predicate: &Predicate{Column: "b", Op: OpIsNull}},
		}, "more than one"},
		{"unknown op", &Filter{Predicate: &Predicate{Column: "a", Op: Operator("regex"), Value: "x"}}, "unknown"},
		{"eq without value", &Filter{Predicate: &Predicate{Column: "a", Op: OpEq}}, "requires a value"},
		{"in with scalar", &Filter{Predicate: &Predicate{Column: "a", Op: OpIn, Value: "x"}}, "list"},
		{"is_null with value", &Filter{Predicate: &Predicate{Column: "a", Op: OpIsNull, Value: 1}}, "takes no value"},
		{"valid tree", &Filter{
			All: []Filter{
				{Predicate: &Predicate{Column: "status", Op: OpNeq, Value: "draft"}},
				{Any: []Filter{
					{Predicate: &Predicate{Column: "type", Op: OpIn, Value: []any{"web", "mobile"}}},
					{Not: &Filter{Predicate: &Predicate{Column: "id", Op: OpIsNotNull}}},
				}},
			},
		}, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := validSpec()
			s.Tables[0].Filter = tc.filter
			err := s.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("want valid, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want problem %q, got %v", tc.wantErr, err)
			}
		})
	}
}
