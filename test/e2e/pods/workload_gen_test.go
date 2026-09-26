package pods

// The production-readiness workload (issue #384): three MySQL tables with
// distinct schemas and primary-key shapes, each driven by its own stochastic
// mutation stream, and an oracle that records what those streams actually
// committed. The rest of the #362 matrix (chaos, KEDA, re-slicing,
// maintenance) runs against a live workload built here.
//
// This file is the pure half: table definitions, the random regimes, the
// transaction builder and the oracle. Nothing here touches a database, so
// the generator is unit-tested without a cluster (workload_unit_test.go);
// workload_run_test.go executes what it builds against MySQL.

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"time"
)

// ── profiles ────────────────────────────────────────────────────────────

// workloadProfile sizes a run. The smoke and full profiles share every line
// of the generator; only these numbers differ.
type workloadProfile struct {
	Name        string
	InitialRows int           // rows seeded per table before the pipeline starts
	MeanRate    float64       // long-run mean mutations/s per table
	Duration    time.Duration // live mutation window
	MaxTxnRows  int           // largest transaction, in row mutations
	MaxPayload  int           // largest payload of the large-payload table, bytes
	Settle      time.Duration // how long the sink may take to converge after the window
}

var (
	smokeProfile = workloadProfile{
		Name: "smoke", InitialRows: 2000, MeanRate: 30, Duration: 3 * time.Minute,
		// The race image drains a partitioned table's backlog about one
		// Iceberg commit per cycle; ~10 minutes after the window is normal.
		MaxTxnRows: 150, MaxPayload: 64 << 10, Settle: 30 * time.Minute,
	}
	fullProfile = workloadProfile{
		Name: "full", InitialRows: 1_000_000, MeanRate: 1000, Duration: 30 * time.Minute,
		MaxTxnRows: 2000, MaxPayload: 256 << 10, Settle: 60 * time.Minute,
	}
)

// selectedProfile reads URUTAU_E2E_PROFILE (smoke by default).
func selectedProfile() (workloadProfile, error) {
	switch v := os.Getenv("URUTAU_E2E_PROFILE"); v {
	case "", "smoke":
		return smokeProfile, nil
	case "full":
		return fullProfile, nil
	default:
		return workloadProfile{}, fmt.Errorf("URUTAU_E2E_PROFILE=%q: want smoke or full", v)
	}
}

// workloadSeed reads URUTAU_E2E_SEED, so a failing run can be replayed, or
// derives a fresh seed from the clock.
func workloadSeed() (uint64, error) {
	if v := os.Getenv("URUTAU_E2E_SEED"); v != "" {
		s, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("URUTAU_E2E_SEED=%q: %w", v, err)
		}
		return s, nil
	}
	return uint64(time.Now().UnixNano()), nil
}

// ── tables ──────────────────────────────────────────────────────────────

type tableKind string

const (
	// accountsKind: BIGINT UNSIGNED key cast to uint64 and range-partitioned
	// across two workers, with keys on both sides of 2^63 and the partition
	// boundary above 2^63 (carried over from #406).
	accountsKind tableKind = "accounts"
	// itemsKind: VARCHAR key, range-partitioned across two workers.
	itemsKind tableKind = "items"
	// eventsKind: composite (tenant_id, event_id) key, large payloads, and
	// the periodically backlogged table (folded in from #355).
	eventsKind tableKind = "events"
)

// twoTo63 is 2^63, the first uint64 key an int64 cannot hold.
const twoTo63 = uint64(1) << 63

// accountsSpan is a quarter of the accounts key range: keys fall in
// [2^63-span, 2^63+3*span), so a quarter sit below 2^63 and the two-worker
// boundary (the middle of the observed range) lands near 2^63+span.
const accountsSpan = uint64(1) << 40

// prColumn is one column's canonical text form, as MySQL and Trino render it.
// Both sides of the comparison read the same text, so a value compares equal
// only when the pipeline carried it exactly.
type prColumn struct {
	Name  string
	MySQL string
	Trino string
}

// prTable is one production-readiness table.
type prTable struct {
	Kind    tableKind
	Name    string // MySQL table in the shop schema, unique per run
	Target  string // Iceberg table in the raw namespace, unique per run
	PK      []string
	Workers int
	Cast    map[string]string
	DDL     string
	Keys    []prColumn // canonical text of the key columns
	Vals    []prColumn // canonical text of the value columns; rev is first
	// insertCols is the column list INSERT writes: keys, then values.
	insertCols []string
	// valueCols is the column list UPDATE sets.
	valueCols []string
	// maxPayload bounds the table's payload column, bytes.
	maxPayload int
}

// productionTables returns the three tables for one run. suffix keeps the
// source tables and sink targets unique, so no run replays another's binlog.
func productionTables(suffix string, p workloadProfile) []*prTable {
	md5MySQL := func(c string) string { return "MD5(" + c + ")" }
	md5Trino := func(c string) string { return "lower(to_hex(md5(to_utf8(" + c + "))))" }
	text := func(c string) prColumn {
		return prColumn{Name: c, MySQL: "CAST(" + c + " AS CHAR)", Trino: "CAST(" + c + " AS VARCHAR)"}
	}
	hashed := func(c string) prColumn { return prColumn{Name: c, MySQL: md5MySQL(c), Trino: md5Trino(c)} }

	accounts := &prTable{
		Kind: accountsKind, Name: "pr_accounts_" + suffix, Target: "pr_accounts_" + suffix,
		PK: []string{"id"}, Workers: 2, Cast: map[string]string{"id": "uint64"},
		DDL: `CREATE TABLE %s (
			id      BIGINT UNSIGNED NOT NULL PRIMARY KEY,
			rev     BIGINT          NOT NULL,
			owner   VARCHAR(64)     NOT NULL,
			balance DECIMAL(18,2)   NOT NULL,
			note    VARCHAR(2048)   NULL)`,
		Keys:       []prColumn{text("id")},
		Vals:       []prColumn{text("rev"), text("owner"), text("balance"), hashed("note")},
		insertCols: []string{"id", "rev", "owner", "balance", "note"},
		valueCols:  []string{"rev", "owner", "balance", "note"},
		maxPayload: 2048,
	}
	items := &prTable{
		Kind: itemsKind, Name: "pr_items_" + suffix, Target: "pr_items_" + suffix,
		PK: []string{"sku"}, Workers: 2,
		DDL: `CREATE TABLE %s (
			sku   VARCHAR(40)   NOT NULL PRIMARY KEY,
			rev   BIGINT        NOT NULL,
			qty   INT           NOT NULL,
			price DECIMAL(12,4) NOT NULL,
			descr VARCHAR(4096) NOT NULL)`,
		Keys:       []prColumn{{Name: "sku", MySQL: "sku", Trino: "sku"}},
		Vals:       []prColumn{text("rev"), text("qty"), text("price"), hashed("descr")},
		insertCols: []string{"sku", "rev", "qty", "price", "descr"},
		valueCols:  []string{"rev", "qty", "price", "descr"},
		maxPayload: 4096,
	}
	events := &prTable{
		Kind: eventsKind, Name: "pr_events_" + suffix, Target: "pr_events_" + suffix,
		PK: []string{"tenant_id", "event_id"}, Workers: 1,
		DDL: `CREATE TABLE %s (
			tenant_id INT           NOT NULL,
			event_id  BIGINT        NOT NULL,
			rev       BIGINT        NOT NULL,
			kind      VARCHAR(32)   NOT NULL,
			amount    DECIMAL(20,6) NULL,
			payload   MEDIUMTEXT    NOT NULL,
			PRIMARY KEY (tenant_id, event_id))`,
		Keys:       []prColumn{text("tenant_id"), text("event_id")},
		Vals:       []prColumn{text("rev"), text("kind"), text("amount"), hashed("payload")},
		insertCols: []string{"tenant_id", "event_id", "rev", "kind", "amount", "payload"},
		valueCols:  []string{"rev", "kind", "amount", "payload"},
		maxPayload: p.MaxPayload,
	}
	return []*prTable{accounts, items, events}
}

// ── canonical values ────────────────────────────────────────────────────

// nullText stands for SQL NULL in a canonical row image.
const nullText = "\x00"

// imageSep separates the columns of a canonical row image.
const imageSep = "\x1f"

// fmtDecimal renders units/10^scale exactly as MySQL and Trino print a
// DECIMAL(p, scale): every fractional digit, a leading "0." and a "-" sign.
func fmtDecimal(units int64, scale int) string {
	neg := units < 0
	u := uint64(units)
	if neg {
		u = uint64(-(units + 1)) + 1
	}
	pow := uint64(1)
	for range scale {
		pow *= 10
	}
	s := strconv.FormatUint(u/pow, 10)
	if scale > 0 {
		frac := strconv.FormatUint(u%pow, 10)
		s += "." + strings.Repeat("0", scale-len(frac)) + frac
	}
	if neg {
		s = "-" + s
	}
	return s
}

// payloadText is a 1 MiB pool of printable ASCII. Payloads are slices of it,
// so a 256 KiB payload costs no generation; the rev prefix makes every
// payload distinct.
var payloadText = func() string {
	r := rand.New(rand.NewPCG(0x5eed, 0xa5c11))
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 .,;:-_/"
	b := make([]byte, 1<<20)
	for i := range b {
		b[i] = alphabet[r.IntN(len(alphabet))]
	}
	return string(b)
}()

// payload returns an n-byte ASCII payload tagged with rev.
func payload(r *rand.Rand, rev int64, n int) string {
	tag := "r" + strconv.FormatInt(rev, 10) + ":"
	if n <= len(tag) {
		return tag
	}
	body := n - len(tag)
	off := r.IntN(len(payloadText) - body + 1)
	return tag + payloadText[off:off+body]
}

// md5Hex is MySQL MD5() and Trino lower(to_hex(md5(to_utf8(x)))) of an
// ASCII string.
func md5Hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

// ── keys ────────────────────────────────────────────────────────────────

// keySet is the set of live keys, with O(1) add, remove and uniform pick.
type keySet struct {
	ids  []string
	keys map[string][]any
	idx  map[string]int
}

func newKeySet() *keySet {
	return &keySet{keys: map[string][]any{}, idx: map[string]int{}}
}

func (s *keySet) len() int           { return len(s.ids) }
func (s *keySet) has(id string) bool { _, ok := s.idx[id]; return ok }

func (s *keySet) add(id string, key []any) {
	if s.has(id) {
		return
	}
	s.idx[id] = len(s.ids)
	s.ids = append(s.ids, id)
	s.keys[id] = key
}

func (s *keySet) remove(id string) {
	i, ok := s.idx[id]
	if !ok {
		return
	}
	last := len(s.ids) - 1
	s.ids[i] = s.ids[last]
	s.idx[s.ids[i]] = i
	s.ids = s.ids[:last]
	delete(s.idx, id)
	delete(s.keys, id)
}

func (s *keySet) pick(r *rand.Rand) (string, []any) {
	id := s.ids[r.IntN(len(s.ids))]
	return id, s.keys[id]
}

// keyID joins a key's canonical texts, the oracle's map key. Key texts are
// digits or [0-9A-Z-], so "|" never occurs inside one.
func keyID(texts []string) string { return strings.Join(texts, "|") }

// ── regimes ─────────────────────────────────────────────────────────────

type opKind int

const (
	opInsert opKind = iota
	opUpdate
	opDelete
)

func (k opKind) String() string {
	return [...]string{"insert", "update", "delete"}[k]
}

// regime is one stretch of a stream's behavior. A stream draws a new regime
// every few seconds, so the rate, transaction size, mix and payload size keep
// moving instead of cycling through a fixed scenario list.
type regime struct {
	Until      time.Time
	Rate       float64    // mutations/s
	TxnMean    float64    // mean row mutations per transaction
	Mix        [3]float64 // insert/update/delete weights
	PayloadMax int        // payload upper bound for this regime, bytes
	BurstProb  float64    // chance a transaction starts a back-to-back burst
	Backlog    bool       // an overload episode for the backlogged table
}

func logUniform(r *rand.Rand, lo, hi float64) float64 {
	return math.Exp(math.Log(lo) + r.Float64()*(math.Log(hi)-math.Log(lo)))
}

func clamp(v, lo, hi float64) float64 { return math.Max(lo, math.Min(hi, v)) }

// newRegime draws a regime starting at now. backlog marks an overload
// episode: a much higher rate, larger transactions and full-size payloads,
// more than the pipeline drains while the episode lasts.
func newRegime(r *rand.Rand, p workloadProfile, t *prTable, now time.Time, backlog bool) regime {
	g := regime{
		Rate:       p.MeanRate * clamp(math.Exp(r.NormFloat64()*0.8), 0.05, 8),
		TxnMean:    logUniform(r, 1, math.Max(2, float64(p.MaxTxnRows)/4)),
		PayloadMax: int(float64(t.maxPayload) * logUniform(r, 0.02, 1)),
		BurstProb:  r.Float64() * 0.3,
		Backlog:    backlog,
	}
	base := [3]float64{0.45, 0.35, 0.20}
	for i := range g.Mix {
		g.Mix[i] = base[i] * math.Exp(r.NormFloat64()*0.6)
	}
	dur := 3*time.Second + time.Duration(r.ExpFloat64()*float64(8*time.Second))
	if backlog {
		g.Rate = p.MeanRate * 8
		g.TxnMean = math.Min(float64(p.MaxTxnRows), g.TxnMean*4)
		g.PayloadMax = t.maxPayload
		g.BurstProb = 0.5
		dur = 10*time.Second + time.Duration(r.Float64()*float64(30*time.Second))
	}
	g.Until = now.Add(min(dur, 40*time.Second))
	if g.PayloadMax < 16 {
		g.PayloadMax = 16
	}
	return g
}

// txnRows draws a transaction size: exponential around the regime's mean,
// with an occasional jumbo transaction at the profile's maximum.
func (g regime) txnRows(r *rand.Rand, maxRows int) int {
	if r.Float64() < 0.01 {
		return maxRows
	}
	n := 1 + int(r.ExpFloat64()*(g.TxnMean-1))
	return min(max(n, 1), maxRows)
}

// payloadSize draws a payload size, log-uniform in [16, PayloadMax].
func (g regime) payloadSize(r *rand.Rand) int {
	if g.PayloadMax <= 16 {
		return 16
	}
	return int(logUniform(r, 16, float64(g.PayloadMax)))
}

// delay is the pause after a transaction of n rows: the regime's rate with
// exponential jitter.
func (g regime) delay(r *rand.Rand, n int) time.Duration {
	return time.Duration(float64(n) / g.Rate * r.ExpFloat64() * float64(time.Second))
}

// ── transactions and the oracle ─────────────────────────────────────────

// mutation is one row mutation inside a transaction.
type mutation struct {
	Kind  opKind
	ID    string // oracle key
	Key   []any  // key values, in PK order
	Vals  []any  // value columns (insert: every insertCols value after the keys)
	Image string // canonical image after the mutation (empty for a delete)
	Rev   int64
	Bytes int // payload bytes written
}

// oracleRow is one live row as the oracle expects it.
type oracleRow struct {
	Rev   int64
	Image string
}

// tableGen builds transactions for one table and keeps its oracle. It is
// driven by one goroutine, so it holds no lock.
type tableGen struct {
	t       *prTable
	p       workloadProfile
	r       *rand.Rand
	live    *keySet
	rows    map[string]oracleRow // the oracle: every live row's expected image
	deleted map[string]int64     // keys deleted by this run and not live now → rev of the delete
	recent  []string             // recently deleted ids, reinserted now and then
	rev     int64
	regime  regime
	// nextEvent is the next event_id per tenant (events table).
	nextEvent map[int64]int64
	// accountsBoundary is the expected two-worker partition boundary of the
	// accounts table, derived from the seeded min/max the same way the MySQL
	// chunker splits (0 until the seed is known).
	accountsBoundary uint64
}

func newTableGen(t *prTable, p workloadProfile, seed uint64, stream uint64) *tableGen {
	g := &tableGen{
		t: t, p: p, r: rand.New(rand.NewPCG(seed, stream)),
		live: newKeySet(), rows: map[string]oracleRow{}, deleted: map[string]int64{},
		nextEvent: map[int64]int64{},
	}
	// The seed phase draws payloads too; start from a regime so it has one.
	g.regime = newRegime(g.r, p, t, time.Time{}, false)
	return g
}

// newKey draws a key that is not live. Now and then it reuses a key this run
// deleted, so a delete followed by a reinsert of the same key is exercised.
func (g *tableGen) newKey() (string, []any, []string) {
	if len(g.recent) > 0 && g.r.Float64() < 0.05 {
		i := g.r.IntN(len(g.recent))
		id := g.recent[i]
		g.recent[i] = g.recent[len(g.recent)-1]
		g.recent = g.recent[:len(g.recent)-1]
		if !g.live.has(id) {
			if key, texts, ok := g.keyFromID(id); ok {
				return id, key, texts
			}
		}
	}
	for {
		var key []any
		var texts []string
		switch g.t.Kind {
		case accountsKind:
			id := twoTo63 - accountsSpan + g.r.Uint64N(4*accountsSpan)
			key, texts = []any{id}, []string{strconv.FormatUint(id, 10)}
		case itemsKind:
			const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
			b := []byte("SKU-")
			for range 10 {
				b = append(b, alphabet[g.r.IntN(len(alphabet))])
			}
			key, texts = []any{string(b)}, []string{string(b)}
		case eventsKind:
			tenant := int64(1 + g.r.IntN(64))
			g.nextEvent[tenant]++
			ev := g.nextEvent[tenant]
			key = []any{tenant, ev}
			texts = []string{strconv.FormatInt(tenant, 10), strconv.FormatInt(ev, 10)}
		}
		id := keyID(texts)
		if !g.live.has(id) {
			return id, key, texts
		}
	}
}

// keyFromID rebuilds key values from an oracle id.
func (g *tableGen) keyFromID(id string) ([]any, []string, bool) {
	texts := strings.Split(id, "|")
	switch g.t.Kind {
	case accountsKind:
		v, err := strconv.ParseUint(texts[0], 10, 64)
		return []any{v}, texts, err == nil
	case itemsKind:
		return []any{texts[0]}, texts, true
	case eventsKind:
		a, err1 := strconv.ParseInt(texts[0], 10, 64)
		b, err2 := strconv.ParseInt(texts[1], 10, 64)
		return []any{a, b}, texts, err1 == nil && err2 == nil
	}
	return nil, nil, false
}

// values draws the value columns for rev and returns them with the row's
// canonical image and payload size.
func (g *tableGen) values(rev int64) ([]any, string, int) {
	revText := strconv.FormatInt(rev, 10)
	h := md5Hex
	switch g.t.Kind {
	case accountsKind:
		owner := "owner-" + strconv.Itoa(g.r.IntN(5000))
		cents := g.r.Int64N(2_000_000_000) - 1_000_000_000
		bal := fmtDecimal(cents, 2)
		var note any
		noteText, n := nullText, 0
		if g.r.Float64() < 0.85 {
			n = g.regime.payloadSize(g.r)
			s := payload(g.r, rev, min(n, g.t.maxPayload))
			note, noteText, n = s, h(s), len(s)
		}
		return []any{rev, owner, bal, note}, strings.Join([]string{revText, owner, bal, noteText}, imageSep), n
	case itemsKind:
		qty := int64(g.r.IntN(100_000))
		price := fmtDecimal(g.r.Int64N(10_000_000_000), 4)
		d := payload(g.r, rev, min(g.regime.payloadSize(g.r), g.t.maxPayload))
		return []any{rev, qty, price, d}, strings.Join([]string{revText, strconv.FormatInt(qty, 10), price, h(d)}, imageSep), len(d)
	case eventsKind:
		kinds := [...]string{"created", "paid", "shipped", "refunded", "note", "audit"}
		kind := kinds[g.r.IntN(len(kinds))]
		var amount any
		amountText := nullText
		if g.r.Float64() < 0.7 {
			a := fmtDecimal(g.r.Int64N(2_000_000_000_000)-1_000_000_000_000, 6)
			amount, amountText = a, a
		}
		pl := payload(g.r, rev, min(g.regime.payloadSize(g.r), g.t.maxPayload))
		return []any{rev, kind, amount, pl}, strings.Join([]string{revText, kind, amountText, h(pl)}, imageSep), len(pl)
	}
	panic("unknown table kind " + string(g.t.Kind))
}

// chooseOp draws the next operation kind, pulling the live row count back
// toward the initial size when it drifts too far either way.
func (g *tableGen) chooseOp(initial int) opKind {
	w := g.regime.Mix
	if g.live.len() < initial/2 {
		w[opInsert] *= 3
		w[opDelete] *= 0.2
	} else if g.live.len() > 2*initial {
		w[opInsert] *= 0.2
		w[opDelete] *= 3
	}
	x := g.r.Float64() * (w[0] + w[1] + w[2])
	switch {
	case x < w[0]:
		return opInsert
	case x < w[0]+w[1]:
		return opUpdate
	default:
		return opDelete
	}
}

// buildTxn draws a transaction of up to n mutations. It updates the live key
// set as it goes, so later mutations in the same transaction see earlier
// ones; undo reverts the key set if the transaction does not commit.
func (g *tableGen) buildTxn(n int, onlyInserts bool) (muts []mutation, undo func()) {
	var undos []func()
	for range n {
		kind := opInsert
		if !onlyInserts {
			kind = g.chooseOp(g.p.InitialRows)
		}
		if kind != opInsert && g.live.len() == 0 {
			kind = opInsert
		}
		g.rev++
		m := mutation{Kind: kind, Rev: g.rev}
		switch kind {
		case opInsert:
			id, key, _ := g.newKey()
			m.ID, m.Key = id, key
			m.Vals, m.Image, m.Bytes = g.values(g.rev)
			g.live.add(id, key)
			undos = append(undos, func() { g.live.remove(id) })
		case opUpdate:
			m.ID, m.Key = g.live.pick(g.r)
			m.Vals, m.Image, m.Bytes = g.values(g.rev)
		case opDelete:
			id, key := g.live.pick(g.r)
			m.ID, m.Key = id, key
			g.live.remove(id)
			undos = append(undos, func() { g.live.add(id, key) })
		}
		muts = append(muts, m)
	}
	return muts, func() {
		for i := len(undos) - 1; i >= 0; i-- {
			undos[i]()
		}
	}
}

// commit applies a committed transaction to the oracle.
func (g *tableGen) commit(muts []mutation) {
	for _, m := range muts {
		switch m.Kind {
		case opInsert, opUpdate:
			g.rows[m.ID] = oracleRow{Rev: m.Rev, Image: m.Image}
			delete(g.deleted, m.ID)
		case opDelete:
			delete(g.rows, m.ID)
			g.deleted[m.ID] = m.Rev
			if len(g.recent) < 256 {
				g.recent = append(g.recent, m.ID)
			}
		}
	}
}

// setAccountsBoundary records the expected two-worker boundary from the
// seeded key range: the MySQL chunker splits [min, max] at
// min + (max-min)/2 + 1 (evenBoundaries in internal/source/mysql).
func (g *tableGen) setAccountsBoundary() {
	if g.t.Kind != accountsKind || g.live.len() == 0 {
		return
	}
	lo, hi := uint64(math.MaxUint64), uint64(0)
	for _, id := range g.live.ids {
		v := g.live.keys[id][0].(uint64)
		lo, hi = min(lo, v), max(hi, v)
	}
	g.accountsBoundary = lo + (hi-lo)/uint64(g.t.Workers) + 1
}

// accountsSide classifies an accounts key against 2^63 and the partition
// boundary, so the diagnostics prove mutations landed on every side.
func (g *tableGen) accountsSide(key []any) string {
	v := key[0].(uint64)
	switch {
	case v < twoTo63:
		return "below-2^63"
	case g.accountsBoundary == 0 || v < g.accountsBoundary:
		return "2^63-to-boundary"
	default:
		return "above-boundary"
	}
}
