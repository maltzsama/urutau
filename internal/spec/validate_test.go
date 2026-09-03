package spec

import (
	"strings"
	"testing"

	"github.com/maltzsama/urutau/internal/core"
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

func TestValidateMetadataClosedCatalog(t *testing.T) {
	s := validSpec()
	s.Tables[0].Metadata = []core.MetadataColumn{
		{From: "op", As: "operation"},
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("valid metadata must validate: %v", err)
	}
}

func TestValidateMetadataUnknownKey(t *testing.T) {
	s := validSpec()
	s.Tables[0].Metadata = []core.MetadataColumn{
		{From: "unknown_key", As: "x"},
	}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "unknown key") {
		t.Fatalf("want unknown metadata key error, got %v", err)
	}
}

func TestValidateMetadataDuplicateAs(t *testing.T) {
	s := validSpec()
	s.Tables[0].Metadata = []core.MetadataColumn{
		{From: "op", As: "x"},
		{From: "phase", As: "x"},
	}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("want duplicated as error, got %v", err)
	}
}

func TestValidateMetadataAsCollisionWithPK(t *testing.T) {
	s := validSpec()
	s.Tables[0].PrimaryKey = []string{"id"}
	s.Tables[0].Metadata = []core.MetadataColumn{
		{From: "op", As: "id"},
	}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "primary key") {
		t.Fatalf("want PK collision error, got %v", err)
	}
}

func TestValidateMetadataAsRegex(t *testing.T) {
	s := validSpec()
	s.Tables[0].Metadata = []core.MetadataColumn{
		{From: "op", As: "invalid name!"},
	}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "not a valid") {
		t.Fatalf("want identifier regex error, got %v", err)
	}
}

func TestValidateCastValid(t *testing.T) {
	s := validSpec()
	s.Tables[0].Cast = map[string]string{
		"amount": "float64",
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("valid cast must validate: %v", err)
	}
}

func TestValidateCastInvalidTarget(t *testing.T) {
	s := validSpec()
	s.Tables[0].Cast = map[string]string{
		"amount": "invalid_type",
	}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "cast") {
		t.Fatalf("want cast error, got %v", err)
	}
}

func TestValidateWarningsOpInUpsert(t *testing.T) {
	s := validSpec()
	s.Tables[0].WriteMode = WriteModeUpsert
	s.Tables[0].Metadata = []core.MetadataColumn{
		{From: "op", As: "op"},
	}
	warns := s.Warnings()
	found := false
	for _, w := range warns {
		if strings.Contains(w, "op") && strings.Contains(w, "upsert") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected op-in-upsert advisory warning, got %v", warns)
	}
}
