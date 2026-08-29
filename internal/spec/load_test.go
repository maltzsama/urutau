package spec

import (
	"strings"
	"testing"
)

const sampleYAML = `
pipeline: ibe-mysql
source:
  kind: mysql
  uri: mysql://repl@mysql:3306/ibe
  serverId: "1101"
sink:
  uri: polaris://polaris:8181/api/catalog
  namespace: raw
  defaults:
    writeMode: upsert
    targetFileSize: 128Mi
tables:
  - source: ibe.bookings
    target: raw.bookings
    primaryKey: [id]
    partitionBy: [day(created_at)]
    createIfNotExists: true
  - source: ibe.orders
    target: raw.orders
    primaryKey: [id]
    filter:
      all:
        - {col: status, op: neq, value: draft}
        - any:
            - {col: type, op: in, value: [web, mobile]}
    worker: orders-grp
    writeMode: append
    filterImmutable: true
  - source: ibe.order_items
    target: raw.order_items
    primaryKey: [order_id, line_no]
    worker: orders-grp
`

func TestLoadYAML(t *testing.T) {
	s, err := LoadYAML(strings.NewReader(sampleYAML))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if s.Pipeline != "ibe-mysql" {
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
	if orders.Worker != "orders-grp" || orders.WriteMode != WriteModeAppend || !orders.FilterImmutable {
		t.Fatalf("orders = %+v", orders)
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
