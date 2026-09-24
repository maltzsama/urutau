package pods

import (
	"testing"
	"time"
)

// TestKafkaResumeAcrossCoordinatorRestart is issue #394's baseline check: does
// a Kafka source actually honor its committed cdc.position offsets across a
// coordinator restart, or does correctness merely happen to hold because the
// test never runs long enough to notice?
//
// Reader.Start ignores the resume position it is handed
// (internal/source/kafka/kafka.go), and the kgo client is built with no
// initial offsets while the source declares RecoverablePosition: true — so
// resume appears to rely entirely on re-reading from the client's default
// reset point plus the worker's skipCovered dropping records at or before its
// committed position. This test produces a batch, lets it commit, kills the
// coordinator, produces a second batch, and checks that every record from
// both batches is present in the sink — proving neither "skip nothing (a
// duplicate-that-matters)" nor "resume past uncommitted data (a loss)"
// happened. format: raw is the baseline shape (see kafkaTableSpec's doc
// comment): this is not a Debezium/CDC question, it is an offset/resume
// question that lives below the decoder.
func TestKafkaResumeAcrossCoordinatorRestart(t *testing.T) {
	requirePods(t)

	produce, trino := setupPodEnvKafka(t)
	const pipeline = "pod-kafka-resume"
	topic := "raw-resume-" + uniqueTarget("t")
	target := uniqueTarget("kafka_resume")

	cr := buildKafkaCR(pipeline, testNS, raceImage(), "pod-e2e-source-kafka", "pod-e2e-catalog",
		[]kafkaTableSpec{{Topic: topic, Target: "raw." + target}})
	applyPipeline(t, testNS, pipeline, cr)
	t.Log("applied: kafka source, format raw, 1 topic")

	waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 5*time.Minute)
	stss := workerSTSs(t, testNS, pipeline)
	if len(stss) != 1 {
		t.Fatalf("want 1 worker StatefulSet, got %v", stss)
	}
	waitPodsByPrefix(t, testNS, stss[0]+"-", 1, 5*time.Minute)

	// First batch: produce, then wait for the sink to durably commit it —
	// the coordinator kill below must land after this is true, so the
	// question is squarely about RESUME, not about in-flight loss.
	first := produceRawRecords(t, produce, topic, 0, 200)
	waitKafkaSettled(t, trino, target, first, 3*time.Minute)
	t.Logf("first batch committed: %d records", len(first))

	// Kill the coordinator mid-stream. The worker's session ends, the
	// coordinator restarts, reopens the Kafka source, and must resume
	// consuming the topic from (at least) its committed offsets.
	coord := waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 5*time.Minute)[0]
	deletePod(t, testNS, coord)
	waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 8*time.Minute)
	waitPodsByPrefix(t, testNS, stss[0]+"-", 1, 8*time.Minute)
	t.Log("coordinator SIGKILLed and recreated")

	// Second batch: produced only after the restart, so a resume that skips
	// too much (mistaking these for already-covered) would lose them, and a
	// client that never advanced past its reset point could also duplicate
	// the first batch — either way the oracle below catches it.
	second := produceRawRecords(t, produce, topic, len(first), 200)
	all := append(append([]int(nil), first...), second...)
	waitKafkaSettled(t, trino, target, all, 5*time.Minute)
	t.Logf("second batch committed: %d records (total %d)", len(second), len(all))

	assertNoRaces(t, testNS, pipeline+"-")
	t.Log("kafka resume across coordinator restart: no record lost, no data race")
}
