// Package inflight holds the coordinator's account of what it has sent and
// not yet seen committed.
//
//   - Budget is the process-wide ceiling on serialized batch bytes queued or
//     sent but not acked. A full budget blocks the producer, which is the
//     backpressure down to the reader loop.
//   - PositionIndex is one worker's unacked batches in send order. An ack
//     releases the budget by table position through it.
//   - Checkpoint writes each index's manifest to S3 on an interval.
//
// The package knows positions and byte counts. It knows nothing of workers'
// queues, sessions or routing, which stay in the coordinator.
//
// It is named inflight, not flight, so it does not read as Arrow Flight, which
// the coordinator imports under that name.
package inflight
