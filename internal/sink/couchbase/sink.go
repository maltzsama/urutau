// Package couchbase implements the sink contract on Couchbase: key-value
// document writes where upsert-by-key IS the native operation — no
// equality deletes, no eventual merges. One collection per table, one
// control document per collection carrying the committed position, and the
// pipeline metadata under a reserved "_urutau" sub-object. The orchestration
// never touches gocb types; they stop at the kvStore seam.
package couchbase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	gocb "github.com/couchbase/gocb/v2"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/sink"
)

// defaultScope is the Couchbase scope bare targets land in.
const defaultScope = "_default"

// createdBucketRAMQuota is the quota of a bucket the sink creates itself.
// Production should pre-create buckets with real sizing and replica counts;
// create-if-missing exists so `urutau run` works against an empty cluster.
// Zero replicas because synchronous durability (the sink's "commit ok means
// durable" requirement) is impossible on a single node with replicas —
// see the durability note on commitFast.
const createdBucketRAMQuota = 256

// Sink implements the sink contract on Couchbase. It owns the cluster
// handle; the orchestration consumes only the sink contract.
type Sink struct {
	cluster    *gocb.Cluster
	bucket     *gocb.Bucket
	bucketName string
	scope      string // fallback scope for bare targets
	dur        gocb.DurabilityLevel
	txns       *gocb.Transactions // non-nil only in atomic commit mode

	mu    sync.Mutex
	plans map[string]core.Schema // ref.Target → resolved schema (EnsureTable)

	// now is injectable so control timestamps are testable deterministically.
	now func() time.Time
}

// Open connects to the cluster and ensures the bucket. The connection
// string is the connection truth (couchbase://host[,host]); credentials
// come from the spec's clientId/clientSecret (or their env fallbacks) —
// gocb does not take credentials in the connection string, and Couchbase
// has no anonymous access, so both are required.
func Open(ctx context.Context, cfg sink.Config) (*Sink, error) {
	if cfg.URI == "" {
		return nil, errors.New("couchbase: sink.uri is required")
	}
	if cfg.Namespace == "" {
		return nil, errors.New("couchbase: sink.namespace (bucket) is required")
	}
	user, pass := cfg.Options["client_id"], cfg.Options["client_secret"]
	if user == "" || pass == "" {
		return nil, errors.New("couchbase: sink.clientId and sink.clientSecret are required (cluster credentials)")
	}
	atomic, err := parseCommitMode(cfg.Options["commit_mode"])
	if err != nil {
		return nil, err
	}

	cluster, err := gocb.Connect(cfg.URI, gocb.ClusterOptions{Username: user, Password: pass})
	if err != nil {
		return nil, fmt.Errorf("couchbase: connect: %w", err)
	}
	if err := cluster.WaitUntilReady(15*time.Second, mgmtReady); err != nil {
		_ = cluster.Close(nil)
		return nil, fmt.Errorf("couchbase: cluster not ready: %w", err)
	}
	bucket, err := ensureBucket(ctx, cluster, cfg.Namespace)
	if err != nil {
		_ = cluster.Close(nil)
		return nil, err
	}

	s := &Sink{
		cluster:    cluster,
		bucket:     bucket,
		bucketName: cfg.Namespace,
		scope:      defaultScope,
		dur:        gocb.DurabilityLevelMajority,
		plans:      map[string]core.Schema{},
		now:        time.Now,
	}
	if sc := cfg.Options["scope"]; sc != "" {
		s.scope = sc
	}
	if atomic {
		// Spawns the background transaction bookkeeping (ATR cleanup);
		// one per process, shared by every atomic table writer.
		s.txns = cluster.Transactions()
	}
	return s, nil
}

// kvReady and mgmtReady are the readiness contract: this sink speaks
// management HTTP and KV only (transactions included — ATR bookkeeping is
// KV). gocbcore tracks KV readiness per bucket, not per cluster — the
// cluster wait covers management, the bucket wait covers KV — and waiting
// on unfiltered service lists would block on query/analytics ports the
// sink never opens.
var (
	mgmtReady = &gocb.WaitUntilReadyOptions{
		DesiredState: gocb.ClusterStateOnline,
		ServiceTypes: []gocb.ServiceType{gocb.ServiceTypeManagement},
	}
	kvReady = &gocb.WaitUntilReadyOptions{
		DesiredState: gocb.ClusterStateOnline,
		ServiceTypes: []gocb.ServiceType{gocb.ServiceTypeKeyValue},
	}
)

// ensureBucket returns the bucket handle, creating the bucket if the
// cluster does not have it. The settings are the dev-cluster minimum that
// keeps synchronous durability possible (0 replicas); a bucket that
// already exists is used as-is — its sizing is the operator's decision.
func ensureBucket(ctx context.Context, cluster *gocb.Cluster, name string) (*gocb.Bucket, error) {
	mgr := cluster.Buckets()
	if _, err := mgr.GetBucket(name, nil); err != nil {
		if !errors.Is(err, gocb.ErrBucketNotFound) {
			return nil, fmt.Errorf("couchbase: inspect bucket %q: %w", name, err)
		}
		err = mgr.CreateBucket(gocb.CreateBucketSettings{
			BucketSettings: gocb.BucketSettings{
				Name:         name,
				RAMQuotaMB:   createdBucketRAMQuota,
				NumReplicas:  0,
				BucketType:   gocb.CouchbaseBucketType,
				FlushEnabled: false,
			},
		}, nil)
		if err != nil && !errors.Is(err, gocb.ErrBucketExists) {
			return nil, fmt.Errorf("couchbase: create bucket %q: %w", name, err)
		}
	}
	b := cluster.Bucket(name)
	if err := b.WaitUntilReady(15*time.Second, kvReady); err != nil {
		return nil, fmt.Errorf("couchbase: bucket %q not ready: %w", name, err)
	}
	return b, nil
}

// parseCommitMode resolves the commit_mode option: empty → fast, the two
// declared values, or a loud error. Plain strings: the option crosses the
// registry boundary as a map value; the spec enum validated it already.
func parseCommitMode(v string) (bool, error) {
	switch v {
	case "":
		return false, nil
	case "fast":
		return false, nil
	case "atomic":
		return true, nil
	default:
		return false, fmt.Errorf("couchbase: sink.commitMode %q is unknown (fast | atomic)", v)
	}
}

// ident resolves a target name into scope + collection, falling back to the
// configured scope for bare names — the same shape as the ClickHouse
// sink's database.table resolution.
func (s *Sink) ident(target string) (scope, coll string, err error) {
	before, after, ok := strings.Cut(target, ".")
	if !ok {
		return s.scope, target, nil
	}
	if strings.Contains(after, ".") {
		return "", "", fmt.Errorf("couchbase: target %q: scope.collection expected (one dot)", target)
	}
	return before, after, nil
}

// collection returns the collection handle for an ident, memoized.
func (s *Sink) collection(scope, coll string) *gocb.Collection {
	return s.bucket.Scope(scope).Collection(coll)
}

// validateSchema rejects any field claiming the reserved metadata
// sub-object name — data columns at ensure time, metadata destinations at
// writer-open time.
func validateSchema(schema core.Schema, meta []core.MetadataColumn) error {
	for _, col := range schema.Columns {
		if col.Name == reservedField {
			return fmt.Errorf("couchbase: column %q is reserved by the sink (the metadata sub-object)", col.Name)
		}
	}
	for _, m := range meta {
		if m.As == reservedField {
			return fmt.Errorf("couchbase: metadata destination %q is reserved", m.As)
		}
	}
	return nil
}

// EnsureTable creates the target scope and collection if absent, and
// records the resolved schema the writer will walk. Validation is
// sink-local (boot-time failure, pause-and-alert) following the ClickHouse
// precedent: append-mode tables need a primary key because a document
// cannot be addressed without one, and no field may claim the reserved
// metadata sub-object name.
func (s *Sink) EnsureTable(ctx context.Context, ref core.TableRef, schema core.Schema, _ []string, _ core.CastPolicy, mode dataplane.WriteMode) error {
	scope, coll, err := s.ident(ref.Target)
	if err != nil {
		return err
	}
	if mode == dataplane.AppendMode && len(ref.PrimaryKey) == 0 {
		return fmt.Errorf("couchbase: append table %s requires a primary key — documents are addressed by key", ref.Target)
	}
	if err := validateSchema(schema, nil); err != nil {
		return err
	}
	// Couchbase reserves the leading underscore for system namespaces
	// ("First character must not be _ or %"); a target starting with _
	// would be rejected by the server anyway — reject it here with a
	// message that names the rule.
	if strings.HasPrefix(coll, "_") {
		return fmt.Errorf("couchbase: collection name %q must not start with '_' (reserved by Couchbase)", coll)
	}
	if strings.HasPrefix(scope, "_") && scope != defaultScope {
		return fmt.Errorf("couchbase: scope name %q must not start with '_' (reserved by Couchbase)", scope)
	}

	mgr := s.bucket.CollectionsV2()
	// The default scope is implicit — it cannot (and need not) be created;
	// asking the server returns a 400, not ErrScopeExists.
	if scope != defaultScope {
		if err := mgr.CreateScope(scope, nil); err != nil && !errors.Is(err, gocb.ErrScopeExists) {
			return fmt.Errorf("couchbase: ensure scope %q: %w", scope, err)
		}
	}
	if err := mgr.CreateCollection(scope, coll, nil, nil); err != nil && !errors.Is(err, gocb.ErrCollectionExists) {
		return fmt.Errorf("couchbase: ensure collection %q: %w", coll, err)
	}

	s.mu.Lock()
	s.plans[ref.Target] = schema
	s.mu.Unlock()
	return nil
}

// Writer opens the per-table committer. The plan is assembled here from the
// schema EnsureTable resolved plus this call's cast and metadata columns.
func (s *Sink) Writer(ctx context.Context, ref core.TableRef, cast core.CastPolicy, meta []core.MetadataColumn) (sink.TableWriter, error) {
	scope, coll, err := s.ident(ref.Target)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	schema, ok := s.plans[ref.Target]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("couchbase: table %s was never ensured — ensure it first", ref.Target)
	}
	metaByName := make(map[string]core.MetadataColumn, len(meta))
	for _, m := range meta {
		metaByName[m.As] = m
	}
	if err := validateSchema(core.Schema{}, meta); err != nil {
		return nil, err
	}
	plan := &tablePlan{
		schema:      schema,
		meta:        metaByName,
		cast:        cast,
		pk:          ref.PrimaryKey,
		sourceTable: ref.Source,
	}
	kv := &realKV{coll: s.collection(scope, coll), dur: s.dur}
	var txr txRunner
	if s.txns != nil {
		txr = &realTx{txns: s.txns, coll: s.collection(scope, coll), dur: s.dur}
	}
	return newTableWriter(kv, txr, plan, s.now), nil
}

// Position reads the committed CDC position from the control document — one
// Get, O(1). An absent control document means never written.
func (s *Sink) Position(ctx context.Context, ref core.TableRef) (string, error) {
	scope, coll, err := s.ident(ref.Target)
	if err != nil {
		return "", err
	}
	return positionOf(ctx, &realKV{coll: s.collection(scope, coll), dur: s.dur})
}

// SetProperties merges snapshot-progress properties into the control
// document.
func (s *Sink) SetProperties(ctx context.Context, ref core.TableRef, props map[string]string) error {
	scope, coll, err := s.ident(ref.Target)
	if err != nil {
		return err
	}
	return setProperties(ctx, &realKV{coll: s.collection(scope, coll), dur: s.dur}, ref, props, s.now)
}

// Properties reads the control document's property map (snapshot resume).
func (s *Sink) Properties(ctx context.Context, ref core.TableRef) (map[string]string, error) {
	scope, coll, err := s.ident(ref.Target)
	if err != nil {
		return nil, err
	}
	return propertiesOf(ctx, &realKV{coll: s.collection(scope, coll), dur: s.dur})
}

// Close releases the cluster connection.
func (s *Sink) Close() error { return s.cluster.Close(nil) }

// realKV is the production kvStore: a gocb collection with synchronous
// durability on every mutation. Majority is the floor for "commit ok means
// the write survived" — with 0 replicas it reduces to the active node's
// in-memory acknowledgment, which is the strongest a single node offers.
//
// The writes are one op per document rather than Collection.Do bulk ops:
// bulk ops carry no durability level, and durability is the requirement —
// throughput tuning waits until it can be measured without giving that up.
type realKV struct {
	coll *gocb.Collection
	dur  gocb.DurabilityLevel
}

func (k *realKV) upsert(ctx context.Context, id string, doc any) error {
	_, err := k.coll.Upsert(id, doc, &gocb.UpsertOptions{DurabilityLevel: k.dur, Context: ctx})
	return translateKVErr(err)
}

func (k *realKV) remove(ctx context.Context, id string) error {
	_, err := k.coll.Remove(id, &gocb.RemoveOptions{DurabilityLevel: k.dur, Context: ctx})
	return translateKVErr(err)
}

func (k *realKV) get(ctx context.Context, id string, out any) (bool, error) {
	res, err := k.coll.Get(id, &gocb.GetOptions{Context: ctx})
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return false, nil
		}
		return false, translateKVErr(err)
	}
	if err := res.Content(out); err != nil {
		return false, fmt.Errorf("couchbase: decode %s: %w", id, err)
	}
	return true, nil
}

func translateKVErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, gocb.ErrDocumentNotFound) {
		return errNotFound
	}
	if errors.Is(err, gocb.ErrDurabilityImpossible) {
		return fmt.Errorf("couchbase: durability impossible — a single-node cluster with replicas cannot acknowledge durable writes (create buckets with 0 replicas, or add nodes): %w", err)
	}
	return err
}

// realTx is the production txRunner: a distributed ACID transaction over
// one collection. The transactional upsert is Get→Replace else Insert —
// the attempt context has no native upsert, matching the spike finding.
type realTx struct {
	txns *gocb.Transactions
	coll *gocb.Collection
	dur  gocb.DurabilityLevel
}

func (t *realTx) run(ctx context.Context, fn func(tx kvStore) error) error {
	// TransactionOptions has no context field — expiry is governed by
	// Timeout, and the SDK retries transient failures itself.
	_, err := t.txns.Run(func(ac *gocb.TransactionAttemptContext) error {
		return fn(&txAttempt{ac: ac, coll: t.coll})
	}, &gocb.TransactionOptions{
		DurabilityLevel: t.dur,
		Timeout:         30 * time.Second,
	})
	return err
}

// txAttempt adapts one transaction attempt to the kvStore seam. Not-found
// maps to (false, nil) — never surfaced to the SDK as a failure, which
// would abort the attempt.
type txAttempt struct {
	ac   *gocb.TransactionAttemptContext
	coll *gocb.Collection
}

func (a *txAttempt) upsert(ctx context.Context, id string, doc any) error {
	existing, err := a.ac.Get(a.coll, id)
	if err != nil {
		if !errors.Is(err, gocb.ErrDocumentNotFound) {
			return err
		}
		_, err = a.ac.Insert(a.coll, id, doc)
		return err
	}
	_, err = a.ac.Replace(existing, doc)
	return err
}

func (a *txAttempt) remove(ctx context.Context, id string) error {
	existing, err := a.ac.Get(a.coll, id)
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return errNotFound
		}
		return err
	}
	return a.ac.Remove(existing)
}

func (a *txAttempt) get(ctx context.Context, id string, out any) (bool, error) {
	res, err := a.ac.Get(a.coll, id)
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return false, nil
		}
		return false, err
	}
	if err := res.Content(out); err != nil {
		return false, fmt.Errorf("couchbase: decode %s in transaction: %w", id, err)
	}
	return true, nil
}

func init() {
	driver.RegisterSink("couchbase", func(ctx context.Context, cfg sink.Config) (sink.Sink, error) {
		return Open(ctx, cfg)
	})
}
