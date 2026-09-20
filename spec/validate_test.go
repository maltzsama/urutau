package spec

import (
	"math"
	"strings"
	"testing"

	"github.com/maltzsama/urutau/core"
)

func validSpec() *Spec {
	return &Spec{
		Pipeline: "shop",
		Source:   Source{Kind: "mysql", URI: "mysql://user@localhost:3306/db"},
		Sink:     Sink{URI: "polaris://localhost:8181/api/catalog", Namespace: "raw"},
		Tables: []Table{
			{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}},
		},
	}
}

func TestValidateAcceptsMinimalUpsert(t *testing.T) {
	if err := validSpec().Validate(); err != nil {
		t.Fatalf("minimal spec must validate: %v", err)
	}
}

// The admission webhook validates the inline CDCPipeline spec before the
// operator mounts the referenced Secrets, so it must skip the URI
// requirements (WithoutCredentials); the coordinator still enforces them on
// the resolved spec.
func TestValidateWithoutCredentials(t *testing.T) {
	s := validSpec()
	s.Source.URI = ""
	s.Sink.URI = ""

	if err := s.Validate(); err == nil {
		t.Fatal("Validate accepted a spec with no source.uri/sink.uri")
	}
	if err := s.Validate(WithoutCredentials()); err != nil {
		t.Fatalf("WithoutCredentials rejected a spec whose URIs come from Secrets: %v", err)
	}
}

func TestValidateEnrich(t *testing.T) {
	base := func() *Spec {
		s := validSpec()
		s.Tables[0].Enrich = []Enrich{{
			Table:    "users",
			Source:   EnrichSource{URI: "mysql://refdb/internal", Query: "SELECT id, name FROM users"},
			On:       map[string]string{"user_ref": "id"},
			Select:   []string{"name", "tier"},
			JoinType: "left",
		}}
		return s
	}
	if err := base().Validate(); err != nil {
		t.Fatalf("minimal enrich must validate: %v", err)
	}

	s := base()
	s.Tables[0].Enrich[0].JoinType = ""
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "joinType") {
		t.Fatalf("want joinType required, got %v", err)
	}
	s = base()
	s.Tables[0].Enrich[0].JoinType = "full"
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "joinType") {
		t.Fatalf("want joinType grammar, got %v", err)
	}
	s = base()
	s.Tables[0].Enrich[0].OnColdStart = "retry"
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "onColdStart") {
		t.Fatalf("want onColdStart grammar, got %v", err)
	}
	s = base()
	s.Tables[0].Enrich[0].Refresh = "soon"
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "refresh") {
		t.Fatalf("want refresh duration, got %v", err)
	}
	s = base()
	s.Tables[0].Enrich[0].Source.Query = ""
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "source.query") {
		t.Fatalf("want source.query required, got %v", err)
	}
	// issue #65: an unprefixed as key ("tier") must fail Validate() the same
	// way enrich.New() rejects it at boot (internal/enrich/enrich.go) — not
	// pass validation and fail two layers deeper.
	s = base()
	s.Tables[0].Enrich[0].As = map[string]string{"tier": "user_tier"}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "not prefixed") {
		t.Fatalf("want as-not-prefixed problem, got %v", err)
	}
	s = base()
	s.Tables[0].Enrich[0].Select = []string{"name"}
	s.Tables[0].Enrich[0].As = map[string]string{"users.tier": "user_tier"}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "not in select") {
		t.Fatalf("want as-not-in-select problem, got %v", err)
	}
	s = base()
	s.Tables[0].Enrich[0].As = map[string]string{"users.tier": "user_tier"}
	if err := s.Validate(); err != nil {
		t.Fatalf("prefixed as key matching select must validate, got %v", err)
	}
	s = base()
	s.Tables[0].Enrich = append(s.Tables[0].Enrich, s.Tables[0].Enrich[0])
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("want duplicated reference, got %v", err)
	}

	// CR-044: select is required and its grammar is closed.
	s = base()
	s.Tables[0].Enrich[0].Select = nil
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "select: required") {
		t.Fatalf("want select required, got %v", err)
	}
	s = base()
	s.Tables[0].Enrich[0].Select = []string{"*", "name"}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), `select: "*" must be the only entry`) {
		t.Fatalf("want star-only rule, got %v", err)
	}
	s = base()
	s.Tables[0].Enrich[0].Select = []string{"name", "name"}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), `select: duplicated "name"`) {
		t.Fatalf("want duplicated select, got %v", err)
	}
	// The star sugar is valid, and renames ride along.
	s = base()
	s.Tables[0].Enrich[0].Select = []string{"*"}
	s.Tables[0].Enrich[0].As = map[string]string{"name": "user_name"}
	if err := s.Validate(); err != nil {
		t.Fatalf("star + rename must validate: %v", err)
	}

	s = base()
	s.Tables[0].Enrich[0].MaxRows = -1
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "maxRows") {
		t.Fatalf("want maxRows must be positive, got %v", err)
	}
	s = base()
	s.Tables[0].Enrich[0].MaxRows = 1000
	if err := s.Validate(); err != nil {
		t.Fatalf("positive maxRows must validate: %v", err)
	}
	s = base()
	s.Tables[0].Enrich[0].MaxRows = math.MaxInt32 + 1
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "maxRows") {
		t.Fatalf("want maxRows int32 bound, got %v", err)
	}
}

// commitMode is Couchbase-only (see the field's doc): an unknown value is
// rejected anywhere, and a known value is rejected on any sink but Couchbase.
// The rejection is what makes "fail fast" work — without it the knob
// validates and is then silently dropped by every other sink.
func TestValidateCommitMode(t *testing.T) {
	// Unknown value: rejected regardless of sink type.
	s := validSpec()
	s.Sink.CommitMode = CommitMode("eventual")
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "commitMode") {
		t.Fatalf("want commitMode problem, got %v", err)
	}

	// Known value on a non-Couchbase sink (the default: empty type is
	// normalized to iceberg+rest) must be rejected.
	s = validSpec()
	s.Sink.CommitMode = CommitModeAtomic
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "commitMode") {
		t.Fatalf("want commitMode problem for a non-Couchbase sink, got %v", err)
	}

	// The Couchbase sink accepts both values.
	for _, mode := range []CommitMode{CommitModeFast, CommitModeAtomic} {
		s := validSpec()
		s.Sink.Type = "couchbase"
		s.Sink.CommitMode = mode
		if err := s.Validate(); err != nil {
			t.Errorf("couchbase commitMode %q must validate: %v", mode, err)
		}
	}

	// An empty commitMode stays valid on any sink (it means "sink default").
	if err := validSpec().Validate(); err != nil {
		t.Fatalf("empty commitMode on the default sink must validate: %v", err)
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
	s.Tables = append(s.Tables, Table{Source: "shop.other", Target: "raw.orders", PrimaryKey: []string{"id"}})
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

// Every catalog member must survive spec validation.
func TestValidateMetadataAcceptsEveryCatalogKey(t *testing.T) {
	for _, key := range core.MetadataCatalog {
		s := validSpec()
		s.Tables[0].Metadata = []core.MetadataColumn{{From: key, As: "meta_col"}}
		if err := s.Validate(); err != nil {
			t.Errorf("catalog key %q rejected by spec validation: %v", key, err)
		}
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

func TestValidateBootstrapModes(t *testing.T) {
	// A typo in the mode must be rejected, not silently treated as a full
	// snapshot — an accidental 400M-row backfill is the failure mode.
	s := validSpec()
	s.Tables[0].Bootstrap = &Bootstrap{Mode: BootstrapMode("adopt-verifi")}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "bootstrap.mode") {
		t.Fatalf("want bootstrap.mode problem, got %v", err)
	}

	// Valid modes pass.
	for _, m := range []BootstrapMode{BootstrapSnapshot, Adopt, AdoptVerify} {
		s.Tables[0].Bootstrap = &Bootstrap{Mode: m}
		if err := s.Validate(); err != nil {
			t.Fatalf("mode %q must validate: %v", m, err)
		}
	}
}

func TestValidateBootstrapStartAt(t *testing.T) {
	s := validSpec()
	s.Tables[0].Bootstrap = &Bootstrap{Mode: Adopt, StartAt: StartAtExplicit}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "bootstrap.position") {
		t.Fatalf("want bootstrap.position problem, got %v", err)
	}

	s.Tables[0].Bootstrap.Position = "lsn:0/1A"
	if err := s.Validate(); err != nil {
		t.Fatalf("explicit position must validate: %v", err)
	}

	// A position without startAt=explicit is contradictory: reject.
	s.Tables[0].Bootstrap.StartAt = StartAtCurrent
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "startAt") {
		t.Fatalf("want startAt problem, got %v", err)
	}
}

// 28.1: kafka append-only needs no primary key; upsert does.
func TestValidateKafkaPrimaryKeyByMode(t *testing.T) {
	s := validSpec()
	s.Source.Kind = "kafka"
	s.Tables[0].WriteMode = WriteModeAppend
	s.Tables[0].PrimaryKey = nil
	s.Tables[0].OnDelete = OnDeleteSkip
	s.Tables[0].Columns = map[string]ColumnDecl{"id": {Scalar: "string"}}
	if err := s.Validate(); err != nil {
		t.Fatalf("kafka append without primaryKey must validate: %v", err)
	}
	// upsert requires the key + partitionedByPrimaryKey assertion.
	s.Tables[0].WriteMode = ""
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "partitionedByPrimaryKey") {
		t.Fatalf("want partitionedByPrimaryKey problem for kafka upsert, got %v", err)
	}
	s.Source.PartitionedByPrimaryKey = true
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "primaryKey") {
		t.Fatalf("want primaryKey problem for kafka upsert without key, got %v", err)
	}
	s.Tables[0].PrimaryKey = []string{"id"}
	if err := s.Validate(); err != nil {
		t.Fatalf("kafka upsert with key + partitioned assertion must validate: %v", err)
	}
}

// 28.2: kafka append-only must declare onDelete: skip (no before image).
func TestValidateKafkaAppendOnDelete(t *testing.T) {
	s := validSpec()
	s.Source.Kind = "kafka"
	s.Tables[0].WriteMode = WriteModeAppend
	s.Tables[0].PrimaryKey = nil
	s.Tables[0].Columns = map[string]ColumnDecl{"id": {Scalar: "string"}}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "onDelete") {
		t.Fatalf("want onDelete problem for kafka append, got %v", err)
	}
	s.Tables[0].OnDelete = OnDeleteSkip
	if err := s.Validate(); err != nil {
		t.Fatalf("kafka append with onDelete: skip must validate: %v", err)
	}
}

// 30: append-idempotent needs a monotonic-sequence source (kafka) and a
// transport identity that includes the message coordinate.
func TestValidateAppendIdempotent(t *testing.T) {
	s := validSpec()
	s.Source.Kind = "kafka"
	s.Tables[0].WriteMode = WriteModeAppendIdempotent
	s.Tables[0].PrimaryKey = nil
	s.Tables[0].Columns = map[string]ColumnDecl{"payload": {Scalar: "string"}}
	s.Tables[0].OnDelete = OnDeleteSkip
	s.Tables[0].Metadata = []core.MetadataColumn{
		{From: core.MetaStream, As: "stream_name"},
		{From: core.MetaShard, As: "partition"},
		{From: core.MetaSeq, As: "offset"},
	}
	// No identity -> rejected.
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("want identity problem, got %v", err)
	}
	// Identity without a coordinate (stream only) -> rejected.
	s.Tables[0].Identity = []string{"stream_name"}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "shard or sequence") {
		t.Fatalf("want coordinate problem, got %v", err)
	}
	// Valid identity -> accepted.
	s.Tables[0].Identity = []string{"partition", "offset"}
	if err := s.Validate(); err != nil {
		t.Fatalf("append-idempotent with transport identity must validate: %v", err)
	}
	// Identity pointing at a data column -> rejected.
	s.Tables[0].Identity = []string{"payload"}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "transport metadata") {
		t.Fatalf("want transport-metadata problem, got %v", err)
	}
}

// format avro requires kafka + a schema registry URL.
func TestValidateAvroFormat(t *testing.T) {
	s := validSpec()
	s.Source.Kind = "kafka"
	s.Tables[0].WriteMode = WriteModeAppend
	s.Tables[0].OnDelete = OnDeleteSkip
	s.Tables[0].Columns = map[string]ColumnDecl{"id": {Scalar: "string"}}
	s.Source.Format = "avro"
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "schemaRegistry") {
		t.Fatalf("want schemaRegistry problem, got %v", err)
	}
	s.Source.SchemaRegistry = "http://registry:8081"
	if err := s.Validate(); err != nil {
		t.Fatalf("avro with registry must validate: %v", err)
	}
	// avro on a non-kafka source is rejected.
	s.Source.Kind = "mysql"
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "avro") {
		t.Fatalf("want avro-kind problem, got %v", err)
	}
}

// The pre-decode Avro bytes are Confluent wire format, not an independently
// readable value — unlike raw's opt-in payload column, there is nothing to
// land under that name.
func TestValidateAvroRejectsPayloadColumn(t *testing.T) {
	s := validSpec()
	s.Source.Kind = "kafka"
	s.Source.Format = "avro"
	s.Source.SchemaRegistry = "http://registry:8081"
	s.Tables[0].WriteMode = WriteModeAppend
	s.Tables[0].OnDelete = OnDeleteSkip
	s.Tables[0].Columns = map[string]ColumnDecl{"payload": {Scalar: "string"}}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "payload") {
		t.Fatalf("want a payload-column problem, got %v", err)
	}
}

// from/required extraction attributes are meaningless outside kafka
// raw/avro: a SQL source introspects its own columns, and debezium already
// interprets the whole envelope.
func TestValidateExtractionAttributesRequireKafkaRawOrAvro(t *testing.T) {
	s := validSpec()
	s.Tables[0].Columns = map[string]ColumnDecl{
		"id": {Scalar: "int64", Required: true},
	}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "from/required") {
		t.Fatalf("want a from/required problem for a non-kafka source, got %v", err)
	}

	s.Source.Kind = "kafka"
	s.Source.Format = "raw"
	s.Tables[0].WriteMode = WriteModeAppend
	s.Tables[0].OnDelete = OnDeleteSkip
	if err := s.Validate(); err != nil {
		t.Fatalf("required must validate for kafka raw: %v", err)
	}
}

// A primary key that points into a nested field is rejected: an equality
// delete cannot encode it as a comparable scalar.
func TestValidateNestedPrimaryKeyRejected(t *testing.T) {
	s := validSpec()
	s.Tables[0].PrimaryKey = []string{"address.city"}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "nested") {
		t.Fatalf("want nested-pk problem, got %v", err)
	}
	s.Tables[0].PrimaryKey = []string{"id"}
	if err := s.Validate(); err != nil {
		t.Fatalf("top-level pk must validate: %v", err)
	}
}

func TestValidatePostgresSourceAccepted(t *testing.T) {
	s := &Spec{
		Pipeline: "pg",
		Source: Source{
			Kind:     "postgres",
			SlotName: "test_slot",
			Postgres: &PostgresSource{Host: "localhost", Database: "mydb"},
		},
		Sink:   Sink{URI: "polaris://localhost:8181/api/catalog", Namespace: "raw"},
		Tables: []Table{{Source: "public.users", Target: "raw.users", PrimaryKey: []string{"id"}}},
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("postgres nested config must validate: %v", err)
	}
}

func TestValidatePostgresRequiresHost(t *testing.T) {
	s := &Spec{
		Pipeline: "pg",
		Source: Source{
			Kind:     "postgres",
			SlotName: "test_slot",
			Postgres: &PostgresSource{Database: "mydb"},
		},
		Sink:   Sink{URI: "polaris://localhost:8181/api/catalog", Namespace: "raw"},
		Tables: []Table{{Source: "public.users", Target: "raw.users", PrimaryKey: []string{"id"}}},
	}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "host") {
		t.Fatalf("want host-required problem, got %v", err)
	}
}

func TestValidatePostgresRequiresDatabase(t *testing.T) {
	s := &Spec{
		Pipeline: "pg",
		Source: Source{
			Kind:     "postgres",
			SlotName: "test_slot",
			Postgres: &PostgresSource{Host: "localhost"},
		},
		Sink:   Sink{URI: "polaris://localhost:8181/api/catalog", Namespace: "raw"},
		Tables: []Table{{Source: "public.users", Target: "raw.users", PrimaryKey: []string{"id"}}},
	}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "database") {
		t.Fatalf("want database-required problem, got %v", err)
	}
}

func TestValidatePostgresRejectsURIAndPostgres(t *testing.T) {
	s := &Spec{
		Pipeline: "pg",
		Source: Source{
			Kind:     "postgres",
			URI:      "postgres://localhost/db",
			SlotName: "test_slot",
			Postgres: &PostgresSource{Host: "localhost", Database: "mydb"},
		},
		Sink:   Sink{URI: "polaris://localhost:8181/api/catalog", Namespace: "raw"},
		Tables: []Table{{Source: "public.users", Target: "raw.users", PrimaryKey: []string{"id"}}},
	}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("want mutually-exclusive problem, got %v", err)
	}
}

func TestValidatePostgresRejectsBadPort(t *testing.T) {
	s := &Spec{
		Pipeline: "pg",
		Source: Source{
			Kind:     "postgres",
			SlotName: "test_slot",
			Postgres: &PostgresSource{Host: "localhost", Database: "mydb", Port: 99999},
		},
		Sink:   Sink{URI: "polaris://localhost:8181/api/catalog", Namespace: "raw"},
		Tables: []Table{{Source: "public.users", Target: "raw.users", PrimaryKey: []string{"id"}}},
	}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "port") {
		t.Fatalf("want port problem, got %v", err)
	}
}

func TestValidatePostgresRejectsBadMaxThreads(t *testing.T) {
	s := &Spec{
		Pipeline: "pg",
		Source: Source{
			Kind:     "postgres",
			SlotName: "test_slot",
			Postgres: &PostgresSource{Host: "localhost", Database: "mydb", MaxThreads: 50},
		},
		Sink:   Sink{URI: "polaris://localhost:8181/api/catalog", Namespace: "raw"},
		Tables: []Table{{Source: "public.users", Target: "raw.users", PrimaryKey: []string{"id"}}},
	}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "maxThreads") {
		t.Fatalf("want maxThreads problem, got %v", err)
	}
}

func TestValidatePostgresRejectsBadInitialWaitTime(t *testing.T) {
	s := &Spec{
		Pipeline: "pg",
		Source: Source{
			Kind:     "postgres",
			SlotName: "test_slot",
			Postgres: &PostgresSource{Host: "localhost", Database: "mydb", CDC: &CDCConfig{InitialWaitTime: 5}},
		},
		Sink:   Sink{URI: "polaris://localhost:8181/api/catalog", Namespace: "raw"},
		Tables: []Table{{Source: "public.users", Target: "raw.users", PrimaryKey: []string{"id"}}},
	}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "initialWaitTime") {
		t.Fatalf("want initialWaitTime problem, got %v", err)
	}
	// The minimum itself is accepted.
	s.Source.Postgres.CDC.InitialWaitTime = 30
	if err := s.Validate(); err != nil {
		t.Fatalf("initialWaitTime=30 must be valid, got %v", err)
	}
}

func TestValidatePostgresDiscover(t *testing.T) {
	base := func() *Spec {
		return &Spec{
			Pipeline: "pg",
			Source: Source{
				Kind:     "postgres",
				SlotName: "test_slot",
				Postgres: &PostgresSource{Host: "localhost", Database: "mydb", Discover: true},
			},
			Sink: Sink{URI: "polaris://localhost:8181/api/catalog", Namespace: "raw"},
		}
	}

	// discover with no tables: valid (the source lists them at boot).
	if err := base().Validate(); err != nil {
		t.Fatalf("discover-only must be valid, got %v", err)
	}

	// discover + explicit tables: rejected.
	s := base()
	s.Tables = []Table{{Source: "public.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "discover") {
		t.Fatalf("want a discover/tables conflict, got %v", err)
	}

	// schemas without discover: advisory warning, not an error.
	s = base()
	s.Source.Postgres.Discover = false
	s.Source.Postgres.Schemas = []string{"public"}
	s.Tables = []Table{{Source: "public.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	if err := s.Validate(); err != nil {
		t.Fatalf("schemas without discover must still validate, got %v", err)
	}
	warns := s.Warnings()
	if len(warns) == 0 || !strings.Contains(warns[0], "schemas") {
		t.Fatalf("want a schemas warning, got %v", warns)
	}
}

func TestValidateTableMode(t *testing.T) {
	base := func() *Spec {
		return &Spec{
			Pipeline: "pg",
			Source:   Source{Kind: "postgres", SlotName: "s", Postgres: &PostgresSource{Host: "h", Database: "d"}},
			Sink:     Sink{URI: "polaris://localhost:8181/api/catalog", Namespace: "raw"},
		}
	}
	// incremental without cursor: error.
	s := base()
	s.Tables = []Table{{Source: "public.t", Target: "raw.t", PrimaryKey: []string{"id"}, Mode: "incremental"}}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "cursor") {
		t.Fatalf("incremental without cursor must error, got %v", err)
	}
	// cursor without incremental: error.
	s = base()
	s.Tables = []Table{{Source: "public.t", Target: "raw.t", PrimaryKey: []string{"id"}, Cursor: "id"}}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "cursor") {
		t.Fatalf("cursor without incremental must error, got %v", err)
	}
	// incremental with cursor: valid.
	s = base()
	s.Tables = []Table{{Source: "public.t", Target: "raw.t", PrimaryKey: []string{"id"}, Mode: "incremental", Cursor: "id"}}
	if err := s.Validate(); err != nil {
		t.Fatalf("incremental with cursor must be valid, got %v", err)
	}
}

func TestValidatePostgresRejectsBadSSLMode(t *testing.T) {
	s := &Spec{
		Pipeline: "pg",
		Source: Source{
			Kind:     "postgres",
			SlotName: "test_slot",
			Postgres: &PostgresSource{Host: "localhost", Database: "mydb", SSL: &SSLConfig{Mode: "bogus"}},
		},
		Sink:   Sink{URI: "polaris://localhost:8181/api/catalog", Namespace: "raw"},
		Tables: []Table{{Source: "public.users", Target: "raw.users", PrimaryKey: []string{"id"}}},
	}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "ssl.mode") {
		t.Fatalf("want ssl.mode problem, got %v", err)
	}
}

func TestValidatePostgresRejectsSSLCertWithoutKey(t *testing.T) {
	s := &Spec{
		Pipeline: "pg",
		Source: Source{
			Kind:     "postgres",
			SlotName: "test_slot",
			Postgres: &PostgresSource{Host: "localhost", Database: "mydb", SSL: &SSLConfig{Mode: "require", Cert: "/cert.pem"}},
		},
		Sink:   Sink{URI: "polaris://localhost:8181/api/catalog", Namespace: "raw"},
		Tables: []Table{{Source: "public.users", Target: "raw.users", PrimaryKey: []string{"id"}}},
	}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "cert and ssl.key") {
		t.Fatalf("want cert+key problem, got %v", err)
	}
}

func TestValidatePostgresRejectsSSHMissingHost(t *testing.T) {
	s := &Spec{
		Pipeline: "pg",
		Source: Source{
			Kind:     "postgres",
			SlotName: "test_slot",
			Postgres: &PostgresSource{Host: "localhost", Database: "mydb", SSH: &SSHConfig{Username: "ubuntu"}},
		},
		Sink:   Sink{URI: "polaris://localhost:8181/api/catalog", Namespace: "raw"},
		Tables: []Table{{Source: "public.users", Target: "raw.users", PrimaryKey: []string{"id"}}},
	}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "ssh.host") {
		t.Fatalf("want ssh.host problem, got %v", err)
	}
}

func TestValidatePostgresRejectsSSHMissingAuth(t *testing.T) {
	s := &Spec{
		Pipeline: "pg",
		Source: Source{
			Kind:     "postgres",
			SlotName: "test_slot",
			Postgres: &PostgresSource{Host: "localhost", Database: "mydb", SSH: &SSHConfig{Host: "bastion", Username: "ubuntu"}},
		},
		Sink:   Sink{URI: "polaris://localhost:8181/api/catalog", Namespace: "raw"},
		Tables: []Table{{Source: "public.users", Target: "raw.users", PrimaryKey: []string{"id"}}},
	}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "password or privateKey") {
		t.Fatalf("want ssh auth problem, got %v", err)
	}
}

func TestValidatePostgresAcceptsWithSSLAndSSH(t *testing.T) {
	s := &Spec{
		Pipeline: "pg",
		Source: Source{
			Kind:     "postgres",
			SlotName: "test_slot",
			Postgres: &PostgresSource{
				Host:     "localhost",
				Database: "mydb",
				SSL:      &SSLConfig{Mode: "verify-full", CA: "/ca.pem"},
				SSH:      &SSHConfig{Host: "bastion", Username: "ubuntu", Password: "secret"},
			},
		},
		Sink:   Sink{URI: "polaris://localhost:8181/api/catalog", Namespace: "raw"},
		Tables: []Table{{Source: "public.users", Target: "raw.users", PrimaryKey: []string{"id"}}},
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("postgres with ssl+ssh must validate: %v", err)
	}
}

func TestValidatePostgresRequiresEitherURIOrPostgres(t *testing.T) {
	s := &Spec{
		Pipeline: "pg",
		Source:   Source{Kind: "postgres", SlotName: "test_slot"},
		Sink:     Sink{URI: "polaris://localhost:8181/api/catalog", Namespace: "raw"},
		Tables:   []Table{{Source: "public.users", Target: "raw.users", PrimaryKey: []string{"id"}}},
	}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "source.uri or source.postgres") {
		t.Fatalf("want uri-or-postgres problem, got %v", err)
	}
}

func TestValidatePostgresVerifyCARequiresCA(t *testing.T) {
	for _, mode := range []string{"verify-ca", "verify-full"} {
		s := &Spec{
			Pipeline: "pg",
			Source: Source{
				Kind:     "postgres",
				SlotName: "test_slot",
				Postgres: &PostgresSource{Host: "localhost", Database: "mydb", SSL: &SSLConfig{Mode: mode}},
			},
			Sink:   Sink{URI: "polaris://localhost:8181/api/catalog", Namespace: "raw"},
			Tables: []Table{{Source: "public.users", Target: "raw.users", PrimaryKey: []string{"id"}}},
		}
		if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "ssl.ca") {
			t.Fatalf("mode %q: want ssl.ca required problem, got %v", mode, err)
		}
	}
}

func TestValidatePostgresRequireDoesNotNeedCA(t *testing.T) {
	s := &Spec{
		Pipeline: "pg",
		Source: Source{
			Kind:     "postgres",
			SlotName: "test_slot",
			Postgres: &PostgresSource{Host: "localhost", Database: "mydb", SSL: &SSLConfig{Mode: "require"}},
		},
		Sink:   Sink{URI: "polaris://localhost:8181/api/catalog", Namespace: "raw"},
		Tables: []Table{{Source: "public.users", Target: "raw.users", PrimaryKey: []string{"id"}}},
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("require mode must not need a CA: %v", err)
	}
}

func TestValidatePostgresRejectsSchemeInHost(t *testing.T) {
	s := &Spec{
		Pipeline: "pg",
		Source: Source{
			Kind:     "postgres",
			SlotName: "test_slot",
			Postgres: &PostgresSource{Host: "postgres://localhost", Database: "mydb"},
		},
		Sink:   Sink{URI: "polaris://localhost:8181/api/catalog", Namespace: "raw"},
		Tables: []Table{{Source: "public.users", Target: "raw.users", PrimaryKey: []string{"id"}}},
	}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "bare hostname") {
		t.Fatalf("want bare-hostname problem, got %v", err)
	}
}

func TestValidatePostgresBlockRejectedForOtherKinds(t *testing.T) {
	s := validSpec() // mysql source
	s.Source.URI = ""
	s.Source.Postgres = &PostgresSource{Host: "localhost", Database: "mydb"}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "only valid for kind postgres") {
		t.Fatalf("want kind rejection, got %v", err)
	}
}

func TestValidateColumnFilterUpsertRequiresPK(t *testing.T) {
	s := validSpec() // upsert by default, pk id
	s.Tables[0].ColumnFilter = []string{"name"}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "must include primary key column") {
		t.Fatalf("want pk-in-filter problem, got %v", err)
	}
	// Including the PK is accepted.
	s.Tables[0].ColumnFilter = []string{"id", "name"}
	if err := s.Validate(); err != nil {
		t.Fatalf("pk included must validate: %v", err)
	}
}

func TestValidateColumnFilterAppendAllowsAnySubset(t *testing.T) {
	s := validSpec()
	s.Tables[0].WriteMode = WriteModeAppend
	s.Tables[0].PrimaryKey = nil // no declared key: any subset is fine
	s.Tables[0].ColumnFilter = []string{"name"}
	if err := s.Validate(); err != nil {
		t.Fatalf("append-only column filter without a key must validate: %v", err)
	}
}

func TestValidateColumnFilterAppendWithDeclaredKeyRequiresIt(t *testing.T) {
	// Even in append mode the sink builds a sort order over a declared key,
	// so the key column cannot be filtered out.
	s := validSpec()
	s.Tables[0].WriteMode = WriteModeAppend
	s.Tables[0].ColumnFilter = []string{"name"} // excludes declared pk id
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "must include primary key column") {
		t.Fatalf("want pk-in-filter problem, got %v", err)
	}
}

func TestValidateColumnFilterRejectsDuplicatesAndEmpty(t *testing.T) {
	s := validSpec()
	s.Tables[0].ColumnFilter = []string{"id", "id", ""}
	err := s.Validate()
	if err == nil || !strings.Contains(err.Error(), "duplicated") || !strings.Contains(err.Error(), "empty column name") {
		t.Fatalf("want duplicate+empty problems, got %v", err)
	}
}

func TestValidateFilterRejectsEmptyList(t *testing.T) {
	for _, op := range []Operator{OpIn, OpNotIn} {
		s := validSpec()
		s.Tables[0].Filter = &Filter{Predicate: &Predicate{Column: "v", Op: op, Value: []any{}}}
		if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "at least one value") {
			t.Fatalf("op %q: want empty-list rejection, got %v", op, err)
		}
	}
}

func TestValidateChunkColumnMustBePK(t *testing.T) {
	s := validSpec()
	s.Tables[0].ChunkColumn = "not_pk"
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "not a primary-key column") {
		t.Fatalf("want chunkColumn pk problem, got %v", err)
	}
	s.Tables[0].ChunkColumn = "id"
	if err := s.Validate(); err != nil {
		t.Fatalf("chunkColumn on the pk must validate: %v", err)
	}
}

func TestValidateMaxReconnectAttempts(t *testing.T) {
	s := &Spec{
		Pipeline: "mysql",
		Source:   Source{Kind: "mysql", URI: "mysql://repl@db/shop", ServerID: "1101", MaxReconnectAttempts: -1},
		Sink:     Sink{URI: "polaris://localhost:8181/api/catalog", Namespace: "raw"},
		Tables:   []Table{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}},
	}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "maxReconnectAttempts") {
		t.Fatalf("want maxReconnectAttempts problem, got %v", err)
	}
}
