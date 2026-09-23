package spec

import (
	"strings"
	"testing"
)

const sampleYAML = `
pipeline: shop-mysql
source:
  kind: mysql
  uri: mysql://repl@mysql:3306/shop
  serverId: "1101"
sink:
  uri: polaris://polaris:8181/api/catalog
  namespace: raw
  defaults:
    writeMode: upsert
    targetFileSize: 128Mi
tables:
  - source: shop.bookings
    target: raw.bookings
    primaryKey: [id]
    partitionBy: [day(created_at)]
    createIfNotExists: true
  - source: shop.orders
    target: raw.orders
    primaryKey: [id]
    filter:
      all:
        - {col: status, op: neq, value: draft}
        - any:
            - {col: type, op: in, value: [web, mobile]}
    workers:
      number: 3
      cpu: "2"
      memory: "4Gi"
    writeMode: append
    filterImmutable: true
  - source: shop.order_items
    target: raw.order_items
    primaryKey: [order_id, line_no]
`

func TestLoadYAML(t *testing.T) {
	s, err := LoadYAML(strings.NewReader(sampleYAML))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if s.Pipeline != "shop-mysql" {
		t.Fatalf("pipeline = %q", s.Pipeline)
	}
	if s.Source.Kind != "mysql" || s.Source.ServerID != "1101" {
		t.Fatalf("source = %+v", s.Source)
	}
	if s.Sink.Defaults.WriteMode != WriteModeUpsert || s.Sink.Defaults.TargetFileSize != "128Mi" {
		t.Fatalf("sink defaults = %+v", s.Sink.Defaults)
	}
	if len(s.Tables) != 3 {
		t.Fatalf("tables = %d", len(s.Tables))
	}

	orders := s.Tables[1]
	if orders.WorkerCount() != 3 || orders.Workers.CPU != "2" || orders.Workers.Memory != "4Gi" ||
		orders.WriteMode != WriteModeAppend || !orders.FilterImmutable {
		t.Fatalf("orders = %+v", orders)
	}
	wantGroups := []string{"shop-mysql-raw-orders-0", "shop-mysql-raw-orders-1", "shop-mysql-raw-orders-2"}
	gotGroups := orders.WorkerGroupNames(s.Pipeline)
	if len(gotGroups) != len(wantGroups) {
		t.Fatalf("WorkerGroupNames = %v, want %v", gotGroups, wantGroups)
	}
	for i, g := range wantGroups {
		if gotGroups[i] != g {
			t.Fatalf("WorkerGroupNames[%d] = %q, want %q", i, gotGroups[i], g)
		}
	}
	if orders.Filter == nil || len(orders.Filter.All) != 2 {
		t.Fatalf("orders filter = %+v", orders.Filter)
	}
	anyBranch := orders.Filter.All[1]
	if anyBranch.Any == nil || len(anyBranch.Any) != 1 || anyBranch.Any[0].Predicate == nil {
		t.Fatalf("filter any branch = %+v", anyBranch)
	}
	if got := anyBranch.Any[0].Predicate.Value; got != nil && len(got.([]any)) != 2 {
		t.Fatalf("in-list value = %v", got)
	}

	items := s.Tables[2]
	if len(items.PrimaryKey) != 2 || items.PrimaryKey[0] != "order_id" {
		t.Fatalf("composite key = %v", items.PrimaryKey)
	}

	if err := s.Validate(); err != nil {
		t.Fatalf("sample must validate: %v", err)
	}
}

func TestLoadYAMLInvalid(t *testing.T) {
	if _, err := LoadYAML(strings.NewReader("pipeline: [broken")); err == nil {
		t.Fatal("want error for malformed yaml")
	}
}

func TestLoadYAMLEnvFallback(t *testing.T) {
	const y = `
pipeline: k8s
source:
  kind: postgres
sink:
  namespace: raw
  warehouse: wh
tables:
  - source: shop.orders
    target: raw.orders
    primaryKey: [id]
`

	t.Setenv("URUTAU_SOURCE_URI", "postgres://u@pg:5432/shop")
	t.Setenv("URUTAU_SINK_URI", "http://polaris:8181/api/catalog")
	t.Setenv("URUTAU_SINK_CLIENT_SECRET", "s3cr3t")

	s, err := LoadYAML(strings.NewReader(y))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if s.Source.URI != "postgres://u@pg:5432/shop" {
		t.Fatalf("source.uri = %q, want env value", s.Source.URI)
	}
	if s.Sink.URI != "http://polaris:8181/api/catalog" {
		t.Fatalf("sink.uri = %q, want env value", s.Sink.URI)
	}
	if s.Sink.ClientSecret != "s3cr3t" {
		t.Fatalf("sink.clientSecret = %q, want env value", s.Sink.ClientSecret)
	}
	// No env set: the field stays empty and downstream validation rejects it.
	if s.Sink.ClientID != "" {
		t.Fatalf("sink.clientId = %q, want empty (no env)", s.Sink.ClientID)
	}
}

func TestLoadYAMLEnvDoesNotOverrideInline(t *testing.T) {
	t.Setenv("URUTAU_SOURCE_URI", "postgres://env@pg:5432/shop")
	s, err := LoadYAML(strings.NewReader("source:\n  kind: mysql\n  uri: mysql://inline@m:3306/shop\n"))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if s.Source.URI != "mysql://inline@m:3306/shop" {
		t.Fatalf("source.uri = %q, want the inline value to win", s.Source.URI)
	}
}

func TestResolveServerIDSpecWins(t *testing.T) {
	s := &Spec{Source: Source{ServerID: "42"}}
	got, err := s.ResolveServerID(1101)
	if err != nil {
		t.Fatalf("ResolveServerID: %v", err)
	}
	if got != 42 {
		t.Fatalf("ResolveServerID = %d, want the spec's 42 to win over the flag default", got)
	}
}

func TestResolveServerIDFallsBackToFlag(t *testing.T) {
	s := &Spec{} // no source.serverId declared
	got, err := s.ResolveServerID(7)
	if err != nil {
		t.Fatalf("ResolveServerID: %v", err)
	}
	if got != 7 {
		t.Fatalf("ResolveServerID = %d, want the flag default 7 when the spec declares nothing", got)
	}
}

func TestResolveServerIDRejectsNonNumeric(t *testing.T) {
	s := &Spec{Source: Source{ServerID: "not-a-number"}}
	if _, err := s.ResolveServerID(1101); err == nil {
		t.Fatal("ResolveServerID: want error for a non-numeric source.serverId")
	}
}

func TestValidateRejectsNonNumericServerID(t *testing.T) {
	s, err := LoadYAML(strings.NewReader(sampleYAML))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	s.Source.ServerID = "not-a-number"
	err = s.Validate()
	if err == nil {
		t.Fatal("Validate: want error for a non-numeric source.serverId")
	}
	if !strings.Contains(err.Error(), "serverId") {
		t.Fatalf("Validate error %q does not mention serverId", err.Error())
	}
}

func TestValidateAcceptsNumericServerID(t *testing.T) {
	s, err := LoadYAML(strings.NewReader(sampleYAML))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("Validate: %v (sampleYAML's serverId: \"1101\" should be accepted)", err)
	}
}

// WK-001 C6 invariant: the coordinator keys its per-worker confirmed position
// by worker name and folds them into one minimum. That is only correct while
// a worker serves exactly ONE table, which holds because WorkerGroupNames
// embeds the target. This test fails the day the derivation stops embedding
// it (a worker group shared across tables), which would silently fold two
// tables' positions into the same minimum.
func TestWorkerGroupNamesEmbedTarget(t *testing.T) {
	tbl := Table{Target: "raw.orders"}
	const pipeline = "shop-mysql"
	prefix := WorkerGroupPrefix(pipeline, tbl.Target)
	names := tbl.WorkerGroupNames(pipeline)
	if len(names) == 0 {
		t.Fatal("no worker group names")
	}
	// The prefix must be a single DNS label: it names the worker StatefulSet
	// and its headless Service, and each pod's hostname is "<prefix>-<index>".
	if strings.ContainsAny(prefix, "._") || prefix != dnsLabel(prefix) {
		t.Fatalf("worker group prefix %q is not a DNS label", prefix)
	}
	for _, name := range names {
		if !strings.HasPrefix(name, prefix+"-") {
			t.Fatalf("worker group %q does not embed the prefix %q", name, prefix)
		}
		if !strings.Contains(name, dnsLabel(tbl.Target)) {
			t.Fatalf("worker group %q does not embed the (sanitized) target — the coordinator's per-worker confirmed key could fold two tables", name)
		}
	}
}

func TestDNSLabel(t *testing.T) {
	cases := map[string]string{
		"raw.orders":      "raw-orders",
		"Shop.Orders":     "shop-orders",
		"a--b":            "a-b",
		"-lead-trail-":    "lead-trail",
		"1digit":          "w-1digit",
		"":                "w",
		"weird!!chars..x": "weird-chars-x",
	}
	for in, want := range cases {
		if got := dnsLabel(in); got != want {
			t.Errorf("dnsLabel(%q) = %q, want %q", in, got, want)
		}
	}
	// Long names are truncated but stay under the 63-char label limit once
	// the "-<ordinal>" and the StatefulSet's "controller-revision-hash"
	// suffixes are appended.
	if got := dnsLabel(strings.Repeat("a", 100)); len(got) > 52 {
		t.Errorf("dnsLabel(long) = %d chars, want <= 52", len(got))
	}
}
