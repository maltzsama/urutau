package coordinator

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/faultinject"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// phaseServe opens the single listener and serves gRPC (control) + Flight
// (data) on it. It returns one cleanup func — stop the server, then close the
// listener — so the caller defers a single resource, matching the original
// LIFO teardown.
func (c *Coordinator) phaseServe() (func(), error) {
	lis, err := net.Listen("tcp", c.cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("coordinator: listen: %w", err)
	}
	c.log.Info("coordinator listening", "addr", lis.Addr().String())

	grpcServer, err := c.startControlServer(lis)
	if err != nil {
		_ = lis.Close()
		return nil, err
	}
	return func() {
		grpcServer.Stop()
		_ = lis.Close()
	}, nil
}

// phaseStream waits for every expected worker session, opens the source
// reader, injects the resolved schemas and the confirmed-position callback,
// starts the replication stream and the batch pump, starts the lag/dashboard
// loops, and marks the coordinator ready (issue #601). It returns the reader
// the snapshot phase shares; the stream's terminal signal is published on
// c.streamErrs for awaitTerminal.
func (c *Coordinator) phaseStream(ctx context.Context, resume position.Position) (source.Reader, error) {
	// Wait for every expected worker session.
	wait := c.cfg.WaitWorker
	if wait <= 0 {
		wait = 2 * time.Minute
	}
	if err := c.waitWorkers(ctx, wait); err != nil {
		return nil, err
	}
	close(c.booted)

	// Reader + stream, then snapshot — the DBLog loop the collapsed runner
	// runs, routed over the wire instead of an in-process channel.
	rdr, err := c.src.Open(ctx, c.refs)
	if err != nil {
		return nil, err
	}
	// A source that reports decoder drops (Kafka onDecodeError: skip) exposes
	// the running count; the lag loop polls it for the skipped gauge (#602).
	if de, ok := rdr.(interface{ DecodeErrors() int64 }); ok {
		c.decodeErrors = de.DecodeErrors
	}
	// A source that can take the resolved schema (optional interface) gets it
	// now: the source boundary then gates on drift against the native shape
	// and encodes stable batches. Keyed by the TARGET the changes are
	// addressed to.
	if si, ok := rdr.(source.SchemaSetter); ok {
		byTarget := make(map[string]core.Schema, len(c.refs))
		for _, ref := range c.refs {
			if cs, ok := c.canonical[ref.Source]; ok {
				byTarget[ref.Target] = cs
			}
		}
		si.SetSourceSchemas(byTarget)
	}
	// The slot's confirmed point tracks the minimum position workers have
	// durably committed (their Acks), never the decode position — otherwise a
	// crash between decode and commit would lose the in-flight window.
	rdr.SetConfirmed(c.confirmedPosition)

	start := resume
	if start == nil {
		m, err := c.src.InitialPosition(ctx)
		if err != nil {
			rdr.Close()
			return nil, fmt.Errorf("coordinator: initial position: %w", err)
		}
		start = m
	}
	if err := rdr.Start(ctx, start); err != nil {
		rdr.Close()
		return nil, fmt.Errorf("coordinator: start stream: %w", err)
	}
	// Batch-native pump (G0/M4): the reader's batches are forwarded whole and
	// serialized once per batch — no decode back to changes, no per-change
	// one-row Flight batch. The FIFO queue preserves the wire ordering the
	// window protocol needs.
	out, streamErr := sourceBatches(ctx, rdr)
	c.streamErrs = streamErr
	// Lag grows between commits, so the gauge needs its own clock: setting it
	// on the ack path would pin it near zero after every commit and never let
	// it rise. Mirrors the dashboard's on-demand Tables(). Started only now: it
	// reads c.snk and c.tables, which boot writes above, and a loop started
	// earlier raced them (the race image aborted a restarting coordinator on
	// it). Before the pump runs there is no lag to report.
	if c.metrics != nil {
		go c.lagLoop(ctx)
		go c.dashPushLoop(ctx)
	}
	go c.pump(ctx, out)
	// Routing is published and the pump is draining the stream: ready (issue
	// #601). Set before the snapshot so a coordinator mid-snapshot is ready —
	// it serves workers and commits; only a coordinator still booting is not.
	c.readiness.Store(true)
	return rdr, nil
}

// phaseSnapshot starts the snapshot in its own goroutine so the terminal wait
// stays live underneath it, and starts the supervisor. It returns the snapshot
// cancel func (deferred by the caller) and the channel that reports the
// snapshot's outcome.
func (c *Coordinator) phaseSnapshot(ctx context.Context, rdr source.Reader, needsSnapshot []source.TableRef) (context.CancelFunc, <-chan error) {
	// The snapshot runs in its own goroutine: run's terminal select must stay
	// live underneath it. A worker dying mid-snapshot otherwise wedges the run
	// forever — the snapshot loop blocks on waitChunkReadyOr, the session error
	// lands in sessionErrs, and nobody reads it (audit #1).
	snapCtx, snapCancel := context.WithCancel(ctx)
	c.snapshotActive.Store(true)
	// Supervision from the start: mid-snapshot only its delivery rule applies
	// (a table's only worker delivering nothing for the delivery timeout ends
	// the run); the ack-timeout reset waits for the stream.
	go c.supervisor.run(ctx, supervisionConfig(c.cfg), c.terminate)
	snapDone := make(chan error, 1)
	go func() {
		defer c.snapshotActive.Store(false)
		defer snapCancel()
		defer close(snapDone)
		// A cancelled snapshot returns before closeWindow, leaving batches held
		// in an open gate. Release them when the phase ends (normally a no-op —
		// closeWindow already drained each partition) so shutdown does not leak
		// Arrow batches (issue #212). gateHold re-checks the window under gateMu
		// before appending, so clearing it here cannot race a late gate.
		defer c.releaseAllGates()
		snapCfg := snapshot.SnapshotConfig{
			WindowTimeout: c.cfg.WindowTimeout,
			CaughtUpPoll:  c.cfg.CaughtUpPoll,
		}
		for _, ref := range needsSnapshot {
			c.log.Info("coordinator snapshot", "table", ref.Source)
			faultinject.At(faultinject.CoordinatorSnapshotTableStart, "table", ref.Target)
			if err := c.emit(eventlog.KindSnapshotStarted, map[string]any{"table": ref.Source}); err != nil {
				c.log.Warn("coordinator: eventlog emit", "err", err)
			}
			chunker := c.lookupChunker(ref.Target)
			if chunker == nil {
				var cerr error
				chunker, cerr = c.qsrc.NewChunker(ref.Source, strings.Join(ref.PrimaryKey, ","), c.cfg.ChunkSize)
				if cerr != nil {
					snapDone <- fmt.Errorf("coordinator: chunker %s: %w", ref.Source, cerr)
					return
				}
			}
			if err := c.snapshotTable(snapCtx, rdr, chunker, ref, snapCfg); err != nil {
				snapDone <- fmt.Errorf("coordinator: snapshot %s: %w", ref.Source, err)
				return
			}
			if err := c.finishSnapshot(snapCtx, ref); err != nil {
				snapDone <- fmt.Errorf("coordinator: snapshot %s: %w", ref.Source, err)
				return
			}
			c.log.Info("coordinator snapshot done", "table", ref.Source)
			if err := c.emit(eventlog.KindSnapshotDone, map[string]any{"table": ref.Source}); err != nil {
				c.log.Warn("coordinator: eventlog emit", "err", err)
			}
		}
		snapDone <- nil
	}()
	return snapCancel, snapDone
}

// awaitTerminal is the single terminal-wait select shared by the snapshot phase
// and the steady-state loop. extra resolves the wait with done=true on a clean
// snapshot completion; a non-nil error from extra is the snapshot's own failure
// and is reported with reason "snapshot". A nil extra never fires, so the
// steady-state caller blocks until a terminal signal.
//
// The ctx.Err() guard on the stream/session cases applies in BOTH call sites: a
// cancelled run must never race a session defer's context.Canceled into the
// report as a spurious worker failure (audit #11).
func (c *Coordinator) awaitTerminal(ctx context.Context, extra <-chan error) (done bool, err error) {
	select {
	case err := <-extra:
		if err != nil {
			c.emitLog(eventlog.KindJobStopped, terminalFields("snapshot", err))
			return false, err
		}
		return true, nil
	case <-ctx.Done():
		c.gracefulShutdown()
		c.emitLog(eventlog.KindJobStopped, terminalFields("shutdown", ctx.Err()))
		return false, ctx.Err()
	case err := <-c.terminate:
		c.gracefulShutdown()
		c.emitLog(eventlog.KindJobTerminated, terminalFields(terminateReason(err), err))
		return false, err
	case err := <-c.streamErrs:
		if ctx.Err() != nil {
			c.gracefulShutdown()
			return false, ctx.Err()
		}
		return false, c.streamTerminal(err)
	case err := <-c.sessionErrs:
		if ctx.Err() != nil {
			c.gracefulShutdown()
			return false, ctx.Err()
		}
		c.gracefulShutdown()
		c.emitLog(eventlog.KindJobStopped, terminalFields("session", err))
		return false, fmt.Errorf("coordinator: worker session: %w", err)
	}
}
