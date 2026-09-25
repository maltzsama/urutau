package pods

// Cluster-free tests of the production-readiness workload generator: they
// run in plain `go test`, without URUTAU_E2E_PODS.

import (
	"reflect"
	"testing"
	"time"
)

func TestFmtDecimal(t *testing.T) {
	cases := []struct {
		units int64
		scale int
		want  string
	}{
		{1250, 2, "12.50"},
		{-50, 2, "-0.50"},
		{0, 2, "0.00"},
		{5, 4, "0.0005"},
		{-123456789, 6, "-123.456789"},
		{42, 0, "42"},
		{-9223372036854775808, 2, "-92233720368547758.08"},
	}
	for _, c := range cases {
		if got := fmtDecimal(c.units, c.scale); got != c.want {
			t.Errorf("fmtDecimal(%d, %d) = %q, want %q", c.units, c.scale, got, c.want)
		}
	}
}

func TestMD5HexMatchesMySQL(t *testing.T) {
	// SELECT MD5('abc') in MySQL.
	if got := md5Hex("abc"); got != "900150983cd24fb0d6963f7d28e17f72" {
		t.Fatalf("md5Hex(abc) = %s", got)
	}
}

// simulate builds txns transactions per table with no database, committing
// every one, and returns the generators and what they recorded.
func simulate(t *testing.T, seed uint64, txns int) ([]*tableGen, []*tableStats) {
	t.Helper()
	p := smokeProfile
	p.InitialRows = 300
	tables := productionTables("unit", p)
	var gens []*tableGen
	var stats []*tableStats
	for i, tb := range tables {
		g := newTableGen(tb, p, seed, uint64(i)+1)
		for left := p.InitialRows; left > 0; left -= 100 {
			muts, _ := g.buildTxn(min(100, left), true)
			g.commit(muts)
		}
		g.setAccountsBoundary()
		s := newTableStats()
		now := time.Unix(1_700_000_000, 0)
		for k := range txns {
			if now.After(g.regime.Until) {
				g.regime = newRegime(g.r, p, tb, now, tb.Kind == eventsKind && k%50 == 0)
			}
			muts, _ := g.buildTxn(g.regime.txnRows(g.r, p.MaxTxnRows), false)
			g.commit(muts)
			s.recordTxn(g, muts, now)
			now = now.Add(g.regime.delay(g.r, len(muts)))
		}
		gens, stats = append(gens, g), append(stats, s)
	}
	return gens, stats
}

func TestWorkloadGeneratorIsDeterministicPerSeed(t *testing.T) {
	a, _ := simulate(t, 42, 200)
	b, _ := simulate(t, 42, 200)
	c, _ := simulate(t, 43, 200)
	for i := range a {
		if !reflect.DeepEqual(a[i].rows, b[i].rows) {
			t.Fatalf("%s: same seed, different oracle", a[i].t.Kind)
		}
		if reflect.DeepEqual(a[i].rows, c[i].rows) {
			t.Fatalf("%s: different seeds, identical oracle", a[i].t.Kind)
		}
	}
}

// The oracle must stay consistent with the live key set, whatever mix of
// inserts, updates, deletes and reinserts the generator draws.
func TestWorkloadOracleMatchesLiveKeys(t *testing.T) {
	gens, _ := simulate(t, 7, 500)
	for _, g := range gens {
		if len(g.rows) != g.live.len() {
			t.Fatalf("%s: oracle has %d rows, live set %d", g.t.Kind, len(g.rows), g.live.len())
		}
		for id := range g.rows {
			if !g.live.has(id) {
				t.Fatalf("%s: oracle row %s is not live", g.t.Kind, id)
			}
			if _, del := g.deleted[id]; del {
				t.Fatalf("%s: row %s is both live and deleted", g.t.Kind, id)
			}
		}
	}
}

// A rolled-back transaction must leave the key set as it was.
func TestWorkloadUndoRestoresKeySet(t *testing.T) {
	g := newTableGen(productionTables("unit", smokeProfile)[0], smokeProfile, 1, 1)
	seed, _ := g.buildTxn(50, true)
	g.commit(seed)
	before := append([]string(nil), g.live.ids...)
	_, undo := g.buildTxn(40, false)
	undo()
	if g.live.len() != len(before) {
		t.Fatalf("after undo: %d live keys, want %d", g.live.len(), len(before))
	}
	for _, id := range before {
		if !g.live.has(id) {
			t.Fatalf("after undo: key %s lost", id)
		}
	}
}

func TestWorkloadCoversOpsSidesAndSizes(t *testing.T) {
	gens, stats := simulate(t, 99, 400)
	for i, g := range gens {
		s := stats[i]
		for _, op := range []opKind{opInsert, opUpdate, opDelete} {
			if s.Ops[op.String()] == 0 {
				t.Errorf("%s: no %s", g.t.Kind, op)
			}
		}
		if g.t.Kind == accountsKind {
			if g.accountsBoundary <= twoTo63 {
				t.Errorf("accounts boundary %d is not above 2^63", g.accountsBoundary)
			}
			for _, side := range []string{"below-2^63", "2^63-to-boundary", "above-boundary"} {
				for _, op := range []opKind{opInsert, opUpdate, opDelete} {
					if s.SideOps[side][op.String()] == 0 {
						t.Errorf("accounts: no %s on the %s side", op, side)
					}
				}
			}
		}
		if g.t.Kind == eventsKind {
			pl := s.payload.summary()
			if pl.Min > 1024 || pl.Max < float64(smokeProfile.MaxPayload)/2 {
				t.Errorf("events payloads spanned only [%v, %v]", pl.Min, pl.Max)
			}
		}
	}
}

// diffStates must put each kind of divergence in its own category.
func TestDiffStatesCategories(t *testing.T) {
	want := map[string]oracleRow{
		"1": {Rev: 5, Image: "5\x1fa"},
		"2": {Rev: 6, Image: "6\x1fb"},
		"3": {Rev: 7, Image: "7\x1fc"},
		"4": {Rev: 8, Image: "8\x1fd"},
	}
	got := map[string]oracleRow{
		// "1" missing
		"2": {Rev: 3, Image: "3\x1fold"}, // stale
		"3": {Rev: 7, Image: "7\x1fX"},   // incorrect
		"4": {Rev: 8, Image: "8\x1fd"},   // equal
		"5": {Rev: 1, Image: "1\x1fz"},   // resurrected delete
		"6": {Rev: 1, Image: "1\x1fy"},   // never written
	}
	d := diffStates(want, got, []string{"4"}, map[string]int64{"5": 2})
	check := func(name string, l idList, want ...string) {
		t.Helper()
		if l.Count != len(want) || !reflect.DeepEqual(append([]string(nil), l.Sample...), want) {
			t.Errorf("%s = %+v, want %v", name, l, want)
		}
	}
	check("missing", d.Missing, "1")
	check("stale", d.Stale, "2")
	check("incorrect", d.Incorrect, "3")
	check("resurrected", d.ResurrectedDeletes, "5")
	check("extra", d.Extra, "6")
	check("duplicated", d.Duplicated, "4")
	if diffStates(want, want, nil, nil).empty() != true {
		t.Error("identical states must diff empty")
	}
}

func TestDistSummary(t *testing.T) {
	d := newDist()
	for i := 1; i <= 100; i++ {
		d.add(float64(i))
	}
	s := d.summary()
	if s.N != 100 || s.Min != 1 || s.Max != 100 || s.Mean != 50.5 || s.P50 != 50 || s.P90 != 90 || s.P99 != 99 {
		t.Fatalf("summary = %+v", s)
	}
	var total int64
	for _, c := range s.Buckets {
		total += c
	}
	if total != 100 {
		t.Fatalf("buckets hold %d values, want 100", total)
	}
	if s.Buckets["[64,128)"] != 37 {
		t.Fatalf("bucket [64,128) = %d, want 37", s.Buckets["[64,128)"])
	}
}

func TestKeyFromIDRoundTrips(t *testing.T) {
	for _, tb := range productionTables("unit", smokeProfile) {
		g := newTableGen(tb, smokeProfile, 3, 1)
		for range 20 {
			id, key, texts := g.newKey()
			k2, t2, ok := g.keyFromID(id)
			if !ok || !reflect.DeepEqual(key, k2) || !reflect.DeepEqual(texts, t2) {
				t.Fatalf("%s: keyFromID(%s) = %v %v %v, want %v %v", tb.Kind, id, k2, t2, ok, key, texts)
			}
		}
	}
}
