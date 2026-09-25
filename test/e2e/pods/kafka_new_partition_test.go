package pods

import (
	"testing"
	"time"
)

// TestKafkaResumeDiscoversNewPartition covers the review finding on #395's
// original resume fix: a topic under kgo.ConsumePartitions (because its
// resume position names at least one partition) never gets its OTHER
// partitions discovered on its own — kgo's AddConsumeTopics doc comment says
// so explicitly ("if you specified ConsumePartitions, this will not add the
// rest of the partitions for a topic ... until the entire topic is purged").
// Without discoverPartitions/addMissingPartitions (internal/source/kafka),
// records written to a partition added after the last commit would be
// silently skipped forever.
//
// Setup: a 2-partition topic. Only partition 0 gets records before the first
// restart, so the resume position that survives it names ONLY partition 0.
// After the restart, records go to BOTH partitions; the sink must contain
// all of them, proving partition 1 — absent from the resume position — was
// discovered and consumed, not silently dropped.
func TestKafkaResumeDiscoversNewPartition(t *testing.T) {
	requirePods(t)

	produce, trino := setupPodEnvKafka(t)
	const pipeline = "pod-kafka-newpart"
	topic := "raw-newpart-" + uniqueTarget("t")
	target := uniqueTarget("kafka_newpart")
	createKafkaTopic(t, produce, topic, 2)

	cr := buildKafkaCR(pipeline, testNS, raceImage(), "pod-e2e-source-kafka", "pod-e2e-catalog",
		[]kafkaTableSpec{{Topic: topic, Target: "raw." + target}})
	applyPipeline(t, testNS, pipeline, cr)
	t.Log("applied: kafka source, format raw, 1 topic, 2 partitions")

	waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 5*time.Minute)
	stss := workerSTSs(t, testNS, pipeline)
	if len(stss) != 1 {
		t.Fatalf("want 1 worker StatefulSet, got %v", stss)
	}
	waitPodsByPrefix(t, testNS, stss[0]+"-", 1, 5*time.Minute)

	// Only partition 0 gets records before the restart: the committed
	// cdc.position after this will name partition 0 alone, never mentioning
	// partition 1 at all.
	part0First := produceRawRecordsToPartition(t, produce, topic, 0, 0, 100)
	waitKafkaSettled(t, trino, target, part0First, 3*time.Minute)
	t.Logf("partition 0 first batch committed: %d records", len(part0First))

	coord := waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 5*time.Minute)[0]
	deletePod(t, testNS, coord)
	waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 8*time.Minute)
	waitPodsByPrefix(t, testNS, stss[0]+"-", 1, 8*time.Minute)
	t.Log("coordinator SIGKILLed and recreated (resume position names only partition 0)")

	// Now produce to BOTH partitions. Partition 1 never appeared in the
	// resume position — if it is not discovered, these records are lost
	// forever, not merely delayed.
	part0Second := produceRawRecordsToPartition(t, produce, topic, 0, len(part0First), 100)
	part1 := produceRawRecordsToPartition(t, produce, topic, 1, 100000, 100)
	all := append(append(append([]int(nil), part0First...), part0Second...), part1...)
	waitKafkaSettled(t, trino, target, all, 5*time.Minute)
	t.Logf("post-restart: partition 0 +%d, partition 1 +%d (total %d)", len(part0Second), len(part1), len(all))

	assertNoRaces(t, testNS, pipeline+"-")
	t.Log("kafka resume discovers a partition absent from the resume position: no record lost, no data race")
}
