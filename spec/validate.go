package spec

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/maltzsama/urutau/core"
)

var (
	ErrNilFilterNode       = errors.New("filter node is empty")
	ErrAmbiguousFilterNode = errors.New("filter node carries more than one of all/any/not/where")
	// identRe is the closed set of valid destination column identifiers.
	identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	// partitionExprRe is the closed grammar for partitionBy expressions.
	partitionExprRe = regexp.MustCompile(
		`^(?:(day|month|year|hour|identity)\((\w+)\)|(bucket|truncate)\((\d+),\s*(\w+)\))$`,
	)
)

// ValidateOption tunes Validate for callers that cannot see the whole spec.
type ValidateOption func(*validateOptions)

type validateOptions struct {
	// credentialsFromEnv skips the URI requirements because the caller
	// validates an inline spec whose URIs are filled from mounted Secrets at
	// coordinator boot. Used by the admission webhook, which runs before the
	// Secrets exist in the pod it validates for.
	credentialsFromEnv bool
}

// WithoutCredentials makes Validate skip the source.uri and sink.uri
// requirements. The admission webhook needs this: it validates the inline
// CDCPipeline spec, which deliberately leaves the URI/credential fields empty
// (the operator mounts Secrets and the coordinator resolves them at boot).
// Every other caller validates a resolved spec and must NOT use this.
func WithoutCredentials() ValidateOption {
	return func(o *validateOptions) { o.credentialsFromEnv = true }
}

// Validate applies the hard, server-side rules. The same code must validate
// inline specs and resolved specs — validation is single and server-side by
// design.
func (s *Spec) Validate(opts ...ValidateOption) error {
	o := validateOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	var problems []string

	if s.Pipeline == "" {
		problems = append(problems, "pipeline: required")
	}

	// source.kind is validated by the driver registry (admission webhook +
	// driver.OpenSource at boot), not here: the set of known kinds is the set
	// of registered drivers, and spec must stay open to future sources. Here
	// we check only that a kind is present and the kafka-specific shape.
	if s.Source.Kind == "" {
		problems = append(problems, "source.kind: required")
	}
	if s.Source.Kind == "kafka" && s.Source.SnapshotMode != "" && s.Source.SnapshotMode != "none" {
		problems = append(problems, "source.snapshotMode: must be \"none\" for kafka")
	}
	if s.Source.Kind == "postgres" && s.Source.SlotName == "" {
		problems = append(problems, "source.slotName: required for postgres (logical replication slot)")
	}
	if s.Source.ServerID != "" {
		if _, err := strconv.ParseUint(s.Source.ServerID, 10, 32); err != nil {
			problems = append(problems, fmt.Sprintf("source.serverId: %q is not a valid uint32 (the MySQL replication server id)", s.Source.ServerID))
		}
	}
	if s.Source.MaxReconnectAttempts < 0 {
		problems = append(problems, "source.maxReconnectAttempts: must be non-negative")
	}
	// Decoder format: raw is a message-log landing mode (kafka only) and it
	// has no upsert semantics — every message is an insert.
	switch s.Source.Format {
	case "", "debezium", "raw", "avro":
	default:
		problems = append(problems, fmt.Sprintf("source.format: unsupported %q (want debezium | raw | avro)", s.Source.Format))
	}
	if s.Source.Format == "raw" && s.Source.Kind != "kafka" {
		problems = append(problems, "source.format: raw is only valid for kind kafka")
	}
	if s.Source.Format == "avro" {
		if s.Source.Kind != "kafka" {
			problems = append(problems, "source.format: avro is only valid for kind kafka")
		}
		if s.Source.SchemaRegistry == "" {
			problems = append(problems, "source.schemaRegistry: required when format is avro (Confluent-compatible registry base URL)")
		}
	}
	if s.Source.Postgres != nil && s.Source.Kind != "postgres" {
		problems = append(problems, "source.postgres: only valid for kind postgres")
	}
	if !o.credentialsFromEnv && s.Source.URI == "" &&
		(s.Source.Kind != "postgres" || s.Source.Postgres == nil) {
		problems = append(problems, "source.uri or source.postgres: required")
	}
	if s.Source.URI != "" && s.Source.Postgres != nil {
		problems = append(problems, "source.uri and source.postgres: mutually exclusive — use one or the other")
	}
	if s.Source.Postgres != nil {
		validatePostgresSource(s.Source.Postgres, &problems)
	}

	if !o.credentialsFromEnv && s.Sink.URI == "" {
		problems = append(problems, "sink.uri: required")
	}
	// Iceberg treats every dot in a namespace/target as a path separator, so an
	// empty component is a malformed identifier. Other sinks (e.g. ClickHouse)
	// treat the namespace as a single identifier and may accept dots
	// literally, so this check is scoped to the Iceberg sink.
	icebergSink := s.Sink.Type == "" || s.Sink.Type == "iceberg+rest"
	if s.Sink.Namespace == "" {
		problems = append(problems, "sink.namespace: required")
	} else if icebergSink && hasEmptyPathComponent(s.Sink.Namespace) {
		problems = append(problems, fmt.Sprintf("sink.namespace: %q has an empty path component (a namespace level cannot be blank)", s.Sink.Namespace))
	}
	// sink.type is validated by the driver registry (admission webhook +
	// driver.OpenSink at boot), not here. We only normalize the default.
	if s.Sink.Type == "" {
		s.Sink.Type = "iceberg+rest" // the default sink
	}
	switch s.Sink.CommitMode {
	case "", CommitModeFast, CommitModeAtomic:
	default:
		problems = append(problems, fmt.Sprintf("sink.commitMode: unknown %q (fast | atomic)", s.Sink.CommitMode))
	}
	// commitMode is Couchbase-only: a non-empty value on any other sink type
	// is rejected here, not silently ignored at runtime, so the operator
	// learns the knob does nothing instead of discovering it later.
	if s.Sink.CommitMode != "" && !sinkSupportsCommitMode(s.Sink.Type) {
		problems = append(problems, fmt.Sprintf(
			"sink.commitMode: only the Couchbase sink uses commitMode, not sink.type %q", s.Sink.Type))
	}
	if s.Sink.Defaults.TargetFileSize != "" {
		if _, err := ParseBytes(s.Sink.Defaults.TargetFileSize); err != nil {
			problems = append(problems, fmt.Sprintf("sink.defaults.targetFileSize: %v", err))
		}
	}
	validateMaintenance(s.Sink.Maintenance, s.Sink.Type, &problems)

	// A discovery pipeline declares no tables: the source lists them at boot.
	discover := s.Source.Postgres != nil && s.Source.Postgres.Discover
	if len(s.Tables) == 0 && !discover {
		problems = append(problems, "tables: at least one required")
	}
	if discover && len(s.Tables) > 0 {
		problems = append(problems, "source.postgres.discover: cannot be combined with an explicit tables list")
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
		} else if icebergSink && hasEmptyPathComponent(tbl.Target) {
			problems = append(problems, fmt.Sprintf("%s.target: %q has an empty path component (a namespace level or the table name cannot be blank)", p, tbl.Target))
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
		if tbl.Workers != nil {
			if tbl.Workers.Number < 0 {
				problems = append(problems, p+".workers.number: must be >= 0")
			}
			if tbl.Workers.Max < 0 {
				problems = append(problems, p+".workers.max: must be >= 0")
			}
			if tbl.Workers.Max > 0 && tbl.Workers.Max < tbl.WorkerCount() {
				problems = append(problems, fmt.Sprintf("%s.workers.max: %d is below workers.number (%d) — autoscaling never drops below the configured count", p, tbl.Workers.Max, tbl.WorkerCount()))
			}
		}

		// Sync mode: a cursor column selects incremental, and only
		// incremental carries one.
		switch tbl.Mode {
		case "", "cdc":
			if tbl.Cursor != "" {
				problems = append(problems, p+".cursor: only valid with mode: incremental")
			}
		case "incremental":
			if tbl.Cursor == "" {
				problems = append(problems, p+".cursor: required with mode: incremental")
			}
		default:
			problems = append(problems, fmt.Sprintf("%s.mode: unsupported %q (want cdc | incremental)", p, tbl.Mode))
		}

		mode := tbl.WriteMode
		if mode == "" {
			mode = s.Sink.Defaults.WriteMode
		}
		if mode == "" {
			mode = WriteModeUpsert // upsert-first: reflecting state is the default
		}
		isAppend := mode == WriteModeAppend || mode == WriteModeAppendIdempotent
		switch {
		case mode == WriteModeUpsert:
			if len(tbl.PrimaryKey) == 0 {
				problems = append(problems, p+".primaryKey: required when writeMode is upsert")
			}
			for _, pk := range tbl.PrimaryKey {
				// An equality delete must encode the key as a comparable
				// scalar. A path into a nested field (address.city) has no
				// such encoding; promote the field to a top-level column.
				if strings.Contains(pk, ".") {
					problems = append(problems, p+".primaryKey: "+pk+" points into a nested field — primary keys must be top-level scalar columns (promote the field to a column if that is the intent)")
				}
			}
		case isAppend:
			if tbl.Filter != nil && !tbl.FilterImmutable {
				problems = append(problems, p+".filterImmutable: required when writeMode is append and a filter is set")
			}
			// Append-only delete semantics are declared, never inferred. A
			// source that carries no before image on deletes (Kafka
			// tombstones) can only skip; recording is impossible.
			switch tbl.OnDelete {
			case "", OnDeleteRecord:
				if s.Source.Kind == "kafka" {
					problems = append(problems, p+".onDelete: kafka carries no before image on deletes — set onDelete: skip for append-only")
				}
			case OnDeleteSkip:
			default:
				problems = append(problems, fmt.Sprintf("%s.onDelete: unsupported %q (want skip | record)", p, tbl.OnDelete))
			}
			if mode == WriteModeAppendIdempotent {
				validateIdentity(tbl, s.Source.Kind, p, &problems)
			}
		default:
			problems = append(problems, fmt.Sprintf("%s.writeMode: unsupported %q", p, mode))
		}

		// Kafka only orders within a partition: an upsert across partitions
		// could silently apply a stale version of a key. The operator must
		// assert the topics are partitioned by the key.
		if s.Source.Kind == "kafka" && mode == WriteModeUpsert && !s.Source.PartitionedByPrimaryKey {
			problems = append(problems, p+".primaryKey: kafka upsert requires source.partitionedByPrimaryKey — the topics must be partitioned by the key, or the same key in different partitions applies stale versions silently")
		}
		// Raw landing is append-only: there is no update or delete, the log
		// is the data.
		if s.Source.Format == "raw" && mode == WriteModeUpsert {
			problems = append(problems, p+".writeMode: source.format is raw — raw landing is append-only, set writeMode: append")
		}

		validateFilter(tbl.Filter, p+".filter", &problems)
		validateColumnFilter(tbl, p, &problems)
		validateChunkColumn(tbl, p, &problems)
		validateMetadata(tbl, p, &problems)
		validateCast(tbl, p, &problems)
		validateColumns(tbl, s.Source, p, &problems)
		validatePartitionBy(tbl, p, &problems)
		validateBootstrap(tbl, p, &problems)
		validateEnrich(tbl, p, &problems)
	}

	if len(problems) > 0 {
		return fmt.Errorf("spec: %s", strings.Join(problems, "; "))
	}
	return nil
}

// validateMetadata checks the closed metadata rules: catalog membership,
// explicit valid destination name, no repeats, never part of the primary
// key.
func validateMetadata(tbl Table, path string, problems *[]string) {
	seen := map[string]bool{}
	for _, m := range tbl.Metadata {
		if !core.ValidMetadataKey(m.From) {
			*problems = append(*problems, fmt.Sprintf("%s.metadata.from: unknown key %q (catalog: %s)", path, m.From, core.MetadataCatalogNames()))
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

// validateColumns resolves every declared column (scalar or nested
// struct/list/map) at boot, so a grammar mistake — an unknown scalar type,
// a malformed composite shape — fails loudly here instead of surfacing at
// first use inside a source's Introspect (audit #10: every other textual
// grammar in this package is validated at boot, not at runtime).
func validateColumns(tbl Table, src Source, path string, problems *[]string) {
	for name, decl := range tbl.Columns {
		if name == "" {
			*problems = append(*problems, fmt.Sprintf("%s.columns: empty column name", path))
			continue
		}
		if _, err := decl.Resolve(); err != nil {
			*problems = append(*problems, fmt.Sprintf("%s.columns.%s: %v", path, name, err))
		}
		if decl.From != "" || decl.Required {
			if src.Kind != "kafka" || (src.Format != "raw" && src.Format != "avro") {
				*problems = append(*problems, fmt.Sprintf("%s.columns.%s: from/required are only valid for kafka raw/avro sources", path, name))
			}
		}
		// The pre-decode bytes are Confluent wire format, unreadable without
		// the registry: there is no raw blob to land, unlike raw's opt-in
		// payload column.
		if src.Format == "avro" && name == "payload" {
			*problems = append(*problems, fmt.Sprintf("%s.columns.%s: \"payload\" is not valid for format avro — the pre-decode bytes are Confluent wire format, not an independently readable value", path, name))
		}
	}
}

// validateColumnFilter checks the column projection. It must be a set of
// distinct, non-empty names, and it must include every declared primary-key
// column: the sink builds the target table's sort order (and, for upsert, the
// equality key) by column name, so an excluded key column makes the table
// unbuildable — in any write mode. A primary key the source introspects but
// the spec does not declare is enforced at Introspect time instead.
func validateColumnFilter(tbl Table, path string, problems *[]string) {
	if len(tbl.ColumnFilter) == 0 {
		return
	}
	seen := make(map[string]bool, len(tbl.ColumnFilter))
	for _, c := range tbl.ColumnFilter {
		if c == "" {
			*problems = append(*problems, path+".columnFilter: empty column name")
			continue
		}
		if seen[c] {
			*problems = append(*problems, fmt.Sprintf("%s.columnFilter: duplicated %q", path, c))
		}
		seen[c] = true
	}
	for _, pk := range tbl.PrimaryKey {
		if !seen[pk] {
			*problems = append(*problems, fmt.Sprintf(
				"%s.columnFilter: must include primary key column %q (the sink resolves the key and sort order by column name)", path, pk))
		}
	}
}

// validateChunkColumn checks the snapshot chunking column. When set it must
// name a primary-key column: the chunk range must stay routable to the same
// worker as the live stream when workers > 1, which only holds for a key
// column (the same constraint OLake's split column carries).
func validateChunkColumn(tbl Table, path string, problems *[]string) {
	if tbl.ChunkColumn == "" {
		return
	}
	for _, pk := range tbl.PrimaryKey {
		if pk == tbl.ChunkColumn {
			return
		}
	}
	*problems = append(*problems, fmt.Sprintf(
		"%s.chunkColumn: %q is not a primary-key column (chunking must follow the key)", path, tbl.ChunkColumn))
}

// validatePartitionBy checks the closed grammar of partition expressions:
// day/month/year/hour(col), bucket(N, col), truncate(N, col), identity(col).
// Bucket and truncate sizes must be > 0. Unknown transforms are rejected.
func validatePartitionBy(tbl Table, path string, problems *[]string) {
	for _, expr := range tbl.PartitionBy {
		m := partitionExprRe.FindStringSubmatch(strings.TrimSpace(expr))
		if m == nil {
			*problems = append(*problems, fmt.Sprintf(
				"%s.partitionBy: invalid expression %q: want transform(col), bucket(N, col), or truncate(N, col)",
				path, expr))
			continue
		}
		// Validate bucket/truncate size > 0.
		if m[3] != "" {
			n, err := strconv.Atoi(m[4])
			if err != nil || n <= 0 {
				*problems = append(*problems, fmt.Sprintf(
					"%s.partitionBy: %q: bucket/truncate size must be > 0", path, expr))
			}
		}
	}
}

// validateIdentity checks the append-idempotent identity: it must be
// transport metadata (stream/shard/sequence), not content, and it must
// include the uniquely-identifying coordinate (shard or sequence) — the
// guarantee comes from the transport, never from a data column. Only a
// source with a monotonic per-message sequence (kafka) qualifies.
func validateIdentity(tbl Table, kind string, path string, problems *[]string) {
	if kind != "kafka" {
		*problems = append(*problems, path+".writeMode: append-idempotent requires a source with a monotonic per-message sequence (kafka)")
		return
	}
	if len(tbl.Identity) == 0 {
		*problems = append(*problems, path+".identity: required when writeMode is append-idempotent")
		return
	}
	// Destination names of the transport metadata columns, and data columns.
	transportAs := map[string]core.MetadataKey{}
	for _, m := range tbl.Metadata {
		switch m.From {
		case core.MetaStream, core.MetaShard, core.MetaSeq:
			transportAs[m.As] = m.From
		}
	}
	hasCoordinate := false
	for _, name := range tbl.Identity {
		from, ok := transportAs[name]
		if !ok {
			*problems = append(*problems, fmt.Sprintf(
				"%s.identity: %q is not a transport metadata column (stream/shard/sequence) — the identity must be transport, never content", path, name))
			continue
		}
		if from == core.MetaShard || from == core.MetaSeq {
			hasCoordinate = true
		}
	}
	if !hasCoordinate {
		*problems = append(*problems, path+".identity: must include shard or sequence — the transport coordinate that uniquely identifies a message")
	}
}

// validateBootstrap checks the closed bootstrap grammar. An unrecognized
// mode must be rejected, not silently treated as a full snapshot — a typo
// in "adopt-verify" would otherwise trigger a complete backfill against
// the operator's intent.
func validateBootstrap(tbl Table, path string, problems *[]string) {
	if tbl.Bootstrap == nil {
		return
	}
	switch tbl.Bootstrap.Mode {
	case "", BootstrapSnapshot, Adopt, AdoptVerify:
	default:
		*problems = append(*problems, fmt.Sprintf(
			"%s.bootstrap.mode: unsupported %q (want snapshot | adopt | adopt-verify)", path, tbl.Bootstrap.Mode))
	}
	switch tbl.Bootstrap.StartAt {
	case "", StartAtCurrent:
	case StartAtExplicit:
		if tbl.Bootstrap.Position == "" {
			*problems = append(*problems, fmt.Sprintf(
				"%s.bootstrap.position: required when startAt is \"explicit\"", path))
		}
	default:
		*problems = append(*problems, fmt.Sprintf(
			"%s.bootstrap.startAt: unsupported %q (want current | explicit)", path, tbl.Bootstrap.StartAt))
	}
	if tbl.Bootstrap.Position != "" && tbl.Bootstrap.StartAt != StartAtExplicit {
		*problems = append(*problems, fmt.Sprintf(
			"%s.bootstrap.startAt: must be \"explicit\" when a position is set", path))
	}
}

// validateEnrich checks the broadcast reference join declarations.
// Structural only — whether the join columns exist in both schemas is a
// boot-time check in internal/enrich, which holds the reference image and
// the event schema. Every knob that changes correctness (joinType, cold
// start) is closed-grammar here.
func validateEnrich(tbl Table, path string, problems *[]string) {
	if len(tbl.Enrich) == 0 {
		return
	}
	seen := map[string]bool{}
	for i, e := range tbl.Enrich {
		ep := fmt.Sprintf("%s.enrich[%d]", path, i)
		if e.Table == "" {
			*problems = append(*problems, ep+".table: required")
		} else if seen[e.Table] {
			*problems = append(*problems, ep+".table: duplicated %q", e.Table)
		}
		seen[e.Table] = true
		if e.Source.URI == "" {
			*problems = append(*problems, ep+".source.uri: required")
		}
		if e.Source.Query == "" {
			*problems = append(*problems, ep+".source.query: required")
		}
		if len(e.On) == 0 {
			*problems = append(*problems, ep+".on: required (event column → reference column)")
		}
		switch e.JoinType {
		case "left", "left outer", "inner", "left semi", "left anti":
		case "":
			*problems = append(*problems, ep+".joinType: required — there is no universal miss policy (left | left outer | inner | left semi | left anti)")
		default:
			*problems = append(*problems, fmt.Sprintf("%s.joinType: unsupported %q (want left | left outer | inner | left semi | left anti)", ep, e.JoinType))
		}
		switch e.OnColdStart {
		case "", "buffer", "pass", "drop":
		default:
			*problems = append(*problems, fmt.Sprintf("%s.onColdStart: unsupported %q (want buffer | pass | drop)", ep, e.OnColdStart))
		}
		if e.Refresh != "" {
			if _, err := time.ParseDuration(e.Refresh); err != nil {
				*problems = append(*problems, fmt.Sprintf("%s.refresh: %q is not a duration (e.g. 5m)", ep, e.Refresh))
			}
		}
		if e.BufferLimits.MaxEvents < 0 {
			*problems = append(*problems, ep+".bufferLimits.maxEvents: must be positive")
		}
		if e.BufferLimits.MaxWait != "" {
			if _, err := time.ParseDuration(e.BufferLimits.MaxWait); err != nil {
				*problems = append(*problems, fmt.Sprintf("%s.bufferLimits.maxWait: %q is not a duration (e.g. 30s)", ep, e.BufferLimits.MaxWait))
			}
		}
		if e.MaxRows < 0 {
			*problems = append(*problems, ep+".maxRows: must be positive")
		}
		if e.MaxRows > math.MaxInt32 {
			*problems = append(*problems, fmt.Sprintf("%s.maxRows: %d exceeds the int32 row-index limit (%d) — the broadcast join indexes rows as int32", ep, e.MaxRows, math.MaxInt32))
		}
		semiAnti := e.JoinType == "left semi" || e.JoinType == "left anti"
		// select is REQUIRED (except for semi/anti, which emit no reference
		// columns — a select there is a spec error): the user declares what
		// the reference adds, so the sink never receives unlisted columns.
		// "*" is the one sugar (documented as careful-use: it injects
		// everything).
		switch {
		case semiAnti && len(e.Select) > 0:
			*problems = append(*problems, fmt.Sprintf("%s.select: %s emits no reference columns — remove select", ep, e.JoinType))
		case semiAnti:
			// no select expected for semi/anti
		case len(e.Select) == 0:
			*problems = append(*problems, ep+`.select: required — declare the reference columns the event receives ("*" injects all of them; prefer an explicit list)`)
		case len(e.Select) == 1 && e.Select[0] == "*":
			// star projection; any rename is allowed (checked against the
			// query result at first load)
		default:
			sel := make(map[string]bool, len(e.Select))
			for _, s := range e.Select {
				if s == "*" {
					*problems = append(*problems, ep+`.select: "*" must be the only entry`)
					continue
				}
				if s == "" {
					*problems = append(*problems, ep+".select: empty column name")
				}
				if sel[s] {
					*problems = append(*problems, fmt.Sprintf("%s.select: duplicated %q", ep, s))
				}
				sel[s] = true
			}
			// as keys must be prefixed with the reference table name (e.g.,
			// "customers.name"), matching enrich.New()'s actual boot-time
			// check (internal/enrich/enrich.go) — a key without the prefix,
			// or one whose unprefixed suffix isn't in select, passes here
			// silently and fails two layers deeper at enrich boot (issue #65).
			prefix := e.Table + "."
			for ref := range e.As {
				if !strings.HasPrefix(ref, prefix) {
					*problems = append(*problems, fmt.Sprintf("%s.as: renames %q which is not prefixed with %q", ep, ref, prefix))
					continue
				}
				refCol := strings.TrimPrefix(ref, prefix)
				if !sel[refCol] {
					*problems = append(*problems, fmt.Sprintf("%s.as: renames %q whose column %q is not in select", ep, ref, refCol))
				}
			}
		}
		// A destination colliding with an EVENT column is documented
		// overwrite semantics (the reference column wins) — not an error.
		// What IS an error: two renames landing on the same name.
		dest := map[string]bool{}
		for ref, as := range e.As {
			if dest[as] {
				*problems = append(*problems, fmt.Sprintf("%s.as: %q and %q land on the same column", ep, ref, as))
			}
			dest[as] = true
		}
	}
}

// Warnings returns the advisory outcomes of the spec — rules that do not
// reject it but must be surfaced to the operator (eventlog, status).
func (s *Spec) Warnings() []string {
	var warns []string
	if pg := s.Source.Postgres; pg != nil && len(pg.Schemas) > 0 && !pg.Discover {
		warns = append(warns, "source.postgres.schemas: ignored without source.postgres.discover")
	}
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

// validateMaintenance checks Sink.Maintenance's duration and size strings.
// A nil Maintenance or a nil sub-config disables that operation — nothing
// to validate. Interval/duration/age defaults are applied at the point of
// use (the Maintainer), not here: Validate only rejects malformed strings.
//
// Maintenance is an Iceberg-only feature (see Maintenance's doc): a
// maintenance block on any other sink type is rejected here, not silently
// ignored at runtime, so an operator who configures it on a ClickHouse or
// Couchbase sink gets a validation error instead of a feature that looks
// configured but never runs. sinkType is the already-normalized Sink.Type
// (Validate defaults an empty type to the Iceberg sink before this call).
func validateMaintenance(m *Maintenance, sinkType string, problems *[]string) {
	if m == nil {
		return
	}
	if !sinkSupportsMaintenance(sinkType) {
		*problems = append(*problems, fmt.Sprintf(
			"sink.maintenance: only the Iceberg sink supports table maintenance, not sink.type %q", sinkType))
		return
	}
	if c := m.Compaction; c != nil {
		if c.Interval != "" {
			if _, err := time.ParseDuration(c.Interval); err != nil {
				*problems = append(*problems, fmt.Sprintf("sink.maintenance.compaction.interval: %q is not a duration (e.g. 5m)", c.Interval))
			}
		}
		if c.TargetFileSize != "" {
			if _, err := ParseBytes(c.TargetFileSize); err != nil {
				*problems = append(*problems, fmt.Sprintf("sink.maintenance.compaction.targetFileSize: %v", err))
			}
		}
	}
	if e := m.SnapshotExpiry; e != nil {
		if e.Interval != "" {
			if _, err := time.ParseDuration(e.Interval); err != nil {
				*problems = append(*problems, fmt.Sprintf("sink.maintenance.snapshotExpiry.interval: %q is not a duration (e.g. 10m)", e.Interval))
			}
		}
		if e.MaxAge != "" {
			if _, err := time.ParseDuration(e.MaxAge); err != nil {
				*problems = append(*problems, fmt.Sprintf("sink.maintenance.snapshotExpiry.maxAge: %q is not a duration (e.g. 168h)", e.MaxAge))
			}
		}
		if e.RetainLast < 0 {
			*problems = append(*problems, "sink.maintenance.snapshotExpiry.retainLast: must be positive")
		}
	}
	if o := m.OrphanCleanup; o != nil {
		if o.Interval != "" {
			if _, err := time.ParseDuration(o.Interval); err != nil {
				*problems = append(*problems, fmt.Sprintf("sink.maintenance.orphanCleanup.interval: %q is not a duration (e.g. 1h)", o.Interval))
			}
		}
		if o.OlderThan != "" {
			if _, err := time.ParseDuration(o.OlderThan); err != nil {
				*problems = append(*problems, fmt.Sprintf("sink.maintenance.orphanCleanup.olderThan: %q is not a duration (e.g. 72h)", o.OlderThan))
			}
		}
	}
}

// sinkSupportsMaintenance reports whether a sink type supports background
// table maintenance. Only the Iceberg sink does: the feature drives
// iceberg-go's compaction/snapshot-expiry/orphan-cleanup APIs, so the
// Iceberg family is the whole supported set. Today that is exactly
// driver.DefaultSinkType ("iceberg+rest"); a future Iceberg catalog variant
// ("iceberg+glue", …) keeps the "<engine>+<catalog>" naming and is covered
// by the prefix, while a non-Iceberg sink registers under a different name
// and is rejected. spec cannot ask the driver registry (driver imports spec,
// not the reverse), so the sink-type name is matched here, the same way the
// kafka-only format rules above match "kafka" literally.
func sinkSupportsMaintenance(sinkType string) bool {
	return sinkType == "iceberg" || strings.HasPrefix(sinkType, "iceberg+")
}

// sinkSupportsCommitMode reports whether a sink type uses sink.commitMode.
// Only the Couchbase sink does: it is the one sink whose data and position
// commits are two separate writes without a distributed transaction, so the
// knob (fast | atomic) is meaningful only there. Unlike the Iceberg family
// there is no "<engine>+<catalog>" variant, so the name is matched exactly.
// spec cannot ask the driver registry (driver imports spec, not the
// reverse), so the sink-type name is matched here, the same way the
// Iceberg-only maintenance rule and the kafka-only format rules do.
func sinkSupportsCommitMode(sinkType string) bool {
	return sinkType == "couchbase"
}

// byteUnits are the binary (Ki/Mi/Gi/Ti) suffixes ParseBytes accepts, in the
// same style as sink.defaults.targetFileSize's existing "128Mi" spelling —
// matching Kubernetes resource-quantity convention, which operators writing
// this spec are already used to. Longest suffix first so "Ki" is tried
// before a bare unitless match would wrongly consume the "K".
var byteUnits = []struct {
	suffix string
	mult   int64
}{
	{"Ti", 1 << 40},
	{"Gi", 1 << 30},
	{"Mi", 1 << 20},
	{"Ki", 1 << 10},
}

// ParseBytes parses a binary byte-size string ("512Mi", "128Ki", "1Gi", or a
// bare integer for plain bytes) into its value in bytes. Used for
// sink.defaults.targetFileSize and sink.maintenance.compaction.targetFileSize.
func ParseBytes(s string) (int64, error) {
	for _, u := range byteUnits {
		if strings.HasSuffix(s, u.suffix) {
			n, err := strconv.ParseInt(strings.TrimSuffix(s, u.suffix), 10, 64)
			if err != nil || n < 0 {
				return 0, fmt.Errorf("%q is not a valid byte size (e.g. 512Mi)", s)
			}
			// Reject the multiplication overflow before it wraps silently:
			// "9223372036854775807Mi" must be a loud error, not a negative
			// byte count.
			if n > math.MaxInt64/u.mult {
				return 0, fmt.Errorf("%q is too large (overflows int64 bytes)", s)
			}
			return n * u.mult, nil
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%q is not a valid byte size (e.g. 512Mi)", s)
	}
	return n, nil
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
		vals, ok := p.Value.([]any)
		if !ok {
			*problems = append(*problems, fmt.Sprintf("%s: op %q requires a list value", path, p.Op))
		} else if len(vals) == 0 {
			// An empty list is meaningless, and squirrel renders an empty
			// NOT IN as (1=1), which would keep NULL rows the CDC guard
			// drops — reject it before either compiler sees it.
			*problems = append(*problems, fmt.Sprintf("%s: op %q requires at least one value", path, p.Op))
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

// validatePostgresSource checks the structured PostgreSQL source config.
func validatePostgresSource(pg *PostgresSource, problems *[]string) {
	if pg.Host == "" {
		*problems = append(*problems, "source.postgres.host: required")
	} else if strings.Contains(pg.Host, "://") || strings.ContainsAny(pg.Host, "/ \t") {
		*problems = append(*problems, "source.postgres.host: must be a bare hostname or IP (no scheme, path, or whitespace)")
	}
	if pg.Database == "" {
		*problems = append(*problems, "source.postgres.database: required")
	}
	if pg.Port != 0 && (pg.Port < 1 || pg.Port > 65535) {
		*problems = append(*problems, "source.postgres.port: must be 1..65535")
	}
	if pg.MaxThreads != 0 && (pg.MaxThreads < 1 || pg.MaxThreads > 32) {
		*problems = append(*problems, "source.postgres.maxThreads: must be 1..32")
	}
	if pg.RetryCount < 0 {
		*problems = append(*problems, "source.postgres.retryCount: must be non-negative")
	}
	if pg.CDC != nil {
		validateCDCConfig(pg.CDC, problems)
	}
	if pg.SSL != nil {
		validateSSLConfig(pg.SSL, problems)
	}
	if pg.SSH != nil {
		validateSSHConfig(pg.SSH, problems)
	}
}

// validateCDCConfig checks the logical-decoding knobs.
func validateCDCConfig(cdc *CDCConfig, problems *[]string) {
	switch cdc.Plugin {
	case "", "pgoutput", "wal2json":
	default:
		*problems = append(*problems, fmt.Sprintf(
			"source.postgres.cdc.plugin: unsupported %q (want pgoutput | wal2json)", cdc.Plugin))
	}
	if cdc.InitialWaitTime != 0 && cdc.InitialWaitTime < 30 {
		*problems = append(*problems, "source.postgres.cdc.initialWaitTime: must be at least 30 seconds")
	}
}

// hasEmptyPathComponent reports whether a dotted path has an empty level, e.g.
// "a..b", ".a" or "a." — a malformed namespace/target that must fail early
// instead of reaching the catalog as an empty namespace or table name.
func hasEmptyPathComponent(s string) bool {
	for _, part := range strings.Split(s, ".") {
		if part == "" {
			return true
		}
	}
	return false
}

func validateSSLConfig(ssl *SSLConfig, problems *[]string) {
	switch ssl.Mode {
	case "", "disable", "require", "verify-ca", "verify-full":
	default:
		*problems = append(*problems, fmt.Sprintf(
			"source.postgres.ssl.mode: unsupported %q (want disable | require | verify-ca | verify-full)", ssl.Mode))
	}
	// verify-ca/verify-full cannot verify the server without a CA to verify
	// against. (require skips verification, so it needs none.)
	if (ssl.Mode == "verify-ca" || ssl.Mode == "verify-full") && ssl.CA == "" {
		*problems = append(*problems, fmt.Sprintf(
			"source.postgres.ssl.ca: required when mode is %q", ssl.Mode))
	}
	if (ssl.Cert != "" || ssl.Key != "") && (ssl.Cert == "" || ssl.Key == "") {
		*problems = append(*problems, "source.postgres.ssl.cert and ssl.key must be set together")
	}
}

func validateSSHConfig(ssh *SSHConfig, problems *[]string) {
	if ssh.Host == "" {
		*problems = append(*problems, "source.postgres.ssh.host: required")
	}
	if ssh.Port != 0 && (ssh.Port < 1 || ssh.Port > 65535) {
		*problems = append(*problems, "source.postgres.ssh.port: must be 1..65535")
	}
	if ssh.Username == "" {
		*problems = append(*problems, "source.postgres.ssh.username: required")
	}
	if ssh.Password == "" && ssh.PrivateKey == "" {
		*problems = append(*problems, "source.postgres.ssh: password or privateKey required")
	}
}
