// Nested-struct Kafka+Avro round trip (issue #17, decision 2): a Confluent-
// Avro producer writes a nested record (struct + list) to a real Redpanda
// topic, registering the schema in Redpanda's Confluent-compatible schema
// registry; the pipeline (spec declares the same shape via the nested
// spec.ColumnDecl syntax from decision 1) consumes it through the real
// Avro decoder and writes it to Iceberg; Trino reads the nested value back.
//
// Requires the Kafka overlay in addition to the main stack:
//
//	docker compose -f docker-compose.yml -f docker-compose.kafka.yml up -d --wait
//	URUTAU_E2E=1 URUTAU_E2E_KAFKA=1 go test ./test/e2e -run TestNestedStructRoundTrip -v
package e2e

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/maltzsama/urutau/internal/runner"
	icebergsink "github.com/maltzsama/urutau/internal/sink/iceberg"
	"github.com/maltzsama/urutau/spec"
)

const ordersAvroSchema = `{"type":"record","name":"order","fields":[
	{"name":"id","type":"long"},
	{"name":"cust","type":{"type":"record","name":"cust","fields":[
		{"name":"name","type":"string"},
		{"name":"age","type":"long"}
	]}},
	{"name":"tags","type":{"type":"array","items":"string"}}
]}`

// requireKafkaE2E additionally requires the Kafka overlay compose — it is
// not part of the shared stack every other e2e test depends on, so it gets
// its own opt-in gate on top of requireE2E.
func requireKafkaE2E(t *testing.T) {
	t.Helper()
	requireE2E(t)
	if env("URUTAU_E2E_KAFKA", "") == "" {
		t.Skip("URUTAU_E2E_KAFKA not set; skipping the Kafka+Avro round trip (needs docker-compose.kafka.yml)")
	}
}

func TestNestedStructRoundTrip(t *testing.T) {
	requireKafkaE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	registryURL := env("URUTAU_E2E_SCHEMA_REGISTRY", "http://localhost:18081")
	brokerAddr := env("URUTAU_E2E_KAFKA_BROKER", "127.0.0.1:19092")
	topic := "shop.orders-avro"

	// Both the topic (repeated produces would otherwise accumulate messages
	// across runs — this pipeline is append-only, no dedup) and the Iceberg
	// table (whose properties carry the resume offset — a stale table would
	// resume past the fresh messages instead of reading them) must start
	// clean for the row-count assertions below to be meaningful.
	resetKafkaTopic(t, topic)
	dropIcebergTableNamed(t, ctx, "kafka_avro", "orders")

	schemaID := registerAvroSchema(t, registryURL, topic+"-value", ordersAvroSchema)
	produceAvroRecords(t, brokerAddr, topic, schemaID)

	s := loadKafkaAvroPipeline(t, brokerAddr, registryURL, topic)

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	runErr := make(chan error, 1)
	go func() { runErr <- runner.Run(runCtx, s, testConfig()) }()

	// A synchronous boot failure (bad spec, source.Open error) returns from
	// runner.Run almost immediately — surface it now instead of letting
	// waitTrino spin for the full timeout on a pipeline that never started.
	select {
	case err := <-runErr:
		t.Fatalf("runner exited early: %v", err)
	case <-time.After(2 * time.Second):
	}

	nsName := "kafka_avro"
	tblQ := nsName + `."orders"`
	waitTrino(t, ctx, `SELECT count(*) FROM `+tblQ, int64(2))
	assertTrino(t, ctx, `SELECT cust.name FROM `+tblQ+` WHERE id = 1`, "ana")
	assertTrino(t, ctx, `SELECT cust.age FROM `+tblQ+` WHERE id = 1`, int64(30))
	assertTrino(t, ctx, `SELECT cardinality(tags) FROM `+tblQ+` WHERE id = 1`, int64(2))
	assertTrino(t, ctx, `SELECT tags[1] FROM `+tblQ+` WHERE id = 1`, "a")
	assertTrino(t, ctx, `SELECT cust.name FROM `+tblQ+` WHERE id = 2`, "bob")

	stop()
	select {
	case err := <-runErr:
		if err != nil && err != context.Canceled {
			t.Fatalf("runner: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runner did not stop after context cancel")
	}
}

// loadKafkaAvroPipeline declares the same nested shape the Avro schema
// carries (cust: struct{name,age}, tags: list<string>) via the spec's
// nested ColumnDecl syntax (issue #17 decision 1) — this is what makes
// Introspect resolve the correct composite core.Schema for the encoder.
func loadKafkaAvroPipeline(t *testing.T, brokerAddr, registryURL, topic string) *spec.Spec {
	t.Helper()
	yaml := `
pipeline: e2e-kafka-avro
source:
  kind: kafka
  uri: ` + brokerAddr + `
  format: avro
  schemaRegistry: ` + registryURL + `
sink:
  uri: ` + env("URUTAU_E2E_CATALOG", "http://localhost:8181/api/catalog") + `
  namespace: kafka_avro
  warehouse: ` + env("URUTAU_E2E_WAREHOUSE", "quickstart_catalog") + `
  clientId: root
  clientSecret: s3cr3t
  scope: PRINCIPAL_ROLE:ALL
tables:
  - source: ` + topic + `
    target: kafka_avro.orders
    writeMode: append
    onDelete: skip
    createIfNotExists: true
    columns:
      id: int64
      cust:
        struct:
          name: string
          age: int64
      tags:
        list: string
`
	s, err := spec.LoadYAML(strings.NewReader(yaml))
	if err != nil {
		t.Fatalf("load pipeline: %v", err)
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("validate pipeline: %v", err)
	}
	return s
}

// registerAvroSchema posts schemaJSON to the registry under subject and
// returns the assigned schema id — the same Confluent HTTP contract
// decoder.HTTPRegistry.fetch reads from on the consume side.
func registerAvroSchema(t *testing.T, registryURL, subject, schemaJSON string) int {
	t.Helper()
	body, err := json.Marshal(map[string]string{"schema": schemaJSON})
	if err != nil {
		t.Fatalf("marshal register request: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost,
		registryURL+"/subjects/"+subject+"/versions", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("register request: %v", err)
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("register schema: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("register schema: status %d", resp.StatusCode)
	}
	var parsed struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode register response: %v", err)
	}
	return parsed.ID
}

// produceAvroRecords writes two Confluent-Avro-wire-encoded records
// (magic 0x00 + big-endian schema id + Avro binary payload) to topic —
// the same wire shape decoder.Avro.Decode expects on the consume side.
func produceAvroRecords(t *testing.T, brokerAddr, topic string, schemaID int) {
	t.Helper()
	schema, err := avro.Parse(ordersAvroSchema)
	if err != nil {
		t.Fatalf("parse avro schema: %v", err)
	}

	rows := []map[string]any{
		{"id": int64(1), "cust": map[string]any{"name": "ana", "age": int64(30)}, "tags": []any{"a", "b"}},
		{"id": int64(2), "cust": map[string]any{"name": "bob", "age": int64(40)}, "tags": []any{}},
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokerAddr), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatalf("kafka client: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, row := range rows {
		var buf bytes.Buffer
		enc := avro.NewEncoderForSchema(schema, &buf)
		if err := enc.Encode(row); err != nil {
			t.Fatalf("encode avro row: %v", err)
		}
		wire := make([]byte, 5+buf.Len())
		wire[0] = 0x00
		binary.BigEndian.PutUint32(wire[1:5], uint32(schemaID))
		copy(wire[5:], buf.Bytes())

		res := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: wire})
		if err := res.FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
}

// resetKafkaTopic deletes topic if it exists (from a previous run) so this
// run's produce starts at offset 0 — the pipeline has no consumer group in
// Kafka to track position with, and re-reading a topic that accumulated
// messages across runs would inflate the row-count assertions. Auto-create
// on the client side (kgo.AllowAutoTopicCreation, used by the producer and
// by kafka.Source.Open) recreates it on first use.
func resetKafkaTopic(t *testing.T, topic string) {
	t.Helper()
	// Best-effort: "topic not found" on a first run is not a failure.
	_ = exec.Command("docker", "exec", "e2e-redpanda-1", "rpk", "topic", "delete", topic).Run()
}

// dropIcebergTableNamed drops one namespace-qualified Iceberg table so a
// stale schema or resume-position table property never survives into a
// fresh run.
func dropIcebergTableNamed(t *testing.T, ctx context.Context, namespace, tableName string) {
	t.Helper()
	cfg := icebergsink.Config{
		URI:          env("URUTAU_E2E_CATALOG", "http://localhost:8181/api/catalog"),
		Warehouse:    env("URUTAU_E2E_WAREHOUSE", "quickstart_catalog"),
		ClientID:     "root",
		ClientSecret: "s3cr3t",
		Scope:        "PRINCIPAL_ROLE:ALL",
	}
	cat, err := icebergsink.NewCatalog(ctx, cfg)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	_ = cat.DropTable(ctx, table.Identifier{namespace, tableName})
}
