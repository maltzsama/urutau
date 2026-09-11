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

// Validate applies the hard, server-side rules. The same code must validate
// inline specs and resolved specs — validation is single and server-side by
// design.
func (s *Spec) Validate() error {
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
	if s.Source.URI == "" {
		problems = append(problems, "source.uri: required")
	}

	if s.Sink.URI == "" {
		problems = append(problems, "sink.uri: required")
	}
	if s.Sink.Namespace == "" {
		problems = append(problems, "sink.namespace: required")
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
		validateMetadata(tbl, p, &problems)
		validateCast(tbl, p, &problems)
		validateColumns(tbl, p, &problems)
		validatePartitionBy(tbl, p, &problems)
		validateBootstrap(tbl, p, &problems)
		validateEnrich(tbl, p, &problems)
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
	core.MetaStream:      true,
	core.MetaShard:       true,
	core.MetaSeq:         true,
	core.MetaMsgTS:       true,
	core.MetaMsgKey:      true,
	core.MetaHeaders:     true,
}

// validateMetadata checks the closed metadata rules: catalog membership,
// explicit valid destination name, no repeats, never part of the primary
// key.
func validateMetadata(tbl Table, path string, problems *[]string) {
	seen := map[string]bool{}
	for _, m := range tbl.Metadata {
		if !validMetadataKeys[m.From] {
			*problems = append(*problems, fmt.Sprintf("%s.metadata.from: unknown key %q (catalog: op, commit_ts, ingest_ts, position, source_table, phase, stream, shard, sequence, msg_ts, msg_key, headers)", path, m.From))
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
func validateColumns(tbl Table, path string, problems *[]string) {
	for name, decl := range tbl.Columns {
		if name == "" {
			*problems = append(*problems, fmt.Sprintf("%s.columns: empty column name", path))
			continue
		}
		if _, err := decl.Resolve(); err != nil {
			*problems = append(*problems, fmt.Sprintf("%s.columns.%s: %v", path, name, err))
		}
	}
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
			// For explicit select, validate that as keys match the prefixed
			// column names (e.g., "users.name") since buildImage uses prefixed
			// names as the as map keys.
			for ref := range e.As {
				found := false
				for s := range sel {
					if s == ref {
						found = true
						break
					}
				}
				if !found {
					*problems = append(*problems, fmt.Sprintf("%s.as: renames %q which is not in select (use prefixed name like %q)", ep, ref, "table.column"))
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
