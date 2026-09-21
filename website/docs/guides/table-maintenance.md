---
sidebar_position: 2
---

# Table maintenance

v0.2.0 adds background Iceberg table maintenance — compaction, snapshot
expiry, and orphan file cleanup. These run as ephemeral worker Pods in
distributed mode, or in-process in single-process mode, on a configurable
schedule. The coordinator never performs maintenance itself.

## Why it matters

Iceberg tables accumulate small data files over time. Without compaction,
reads get slower and metadata files grow. Without snapshot expiry, the
catalog retains history you don't need. Without orphan cleanup, deleted
files linger on disk and rack up storage costs.

Urutau automates all three so you don't have to schedule external jobs.

## Enabling maintenance

Add a `sink.maintenance` block to your pipeline spec:

```yaml
sink:
  type: iceberg+rest
  uri: http://catalog:8181
  warehouse: s3://warehouse
  tables:
    - source: shop.orders
      target: bronze.orders

  maintenance:
    enabled: true
    compaction:
      interval: 5m
      targetFileSize: 512Mi
      minInputFiles: 5
      minCommitInterval: 15m
      safemode:
        enabled: true
        maxDowntime: 10m
        checkInterval: 1m
    snapshotExpiry:
      interval: 10m
      retainLast: 1
      maxAge: 168h
    orphanCleanup:
      interval: 1h
      olderThan: 72h
```

Each sub-block (`compaction`, `snapshotExpiry`, `orphanCleanup`) is
independently optional. You can enable compaction without enabling the
other two, or any combination.

If `maintenance.enabled` is `false` or absent, no maintenance runs.

Every `interval` below is a **throttle measured from the previous run's
completion**, not a fixed start-time schedule: a run that takes longer than its
interval does not immediately re-fire — the next run starts one full interval
after the last one *finished*. So `interval: 5m` means "at most once every 5
minutes of quiet time between runs", not "at 5-minute wall-clock boundaries".
This is the same in single-process and distributed mode.

## Compaction

Compaction merges small files into `targetFileSize`-sized outputs using
`iceberg-go`'s bin-pack planner. It reads the table metadata, selects
partitions with enough small files (`minInputFiles`), and writes new
data files. If a compaction output is still under `targetFileSize`, it
is written anyway — this is the expected behavior for low-volume tables.

### Configuration

| Field | Default | Description |
| --- | --- | --- |
| `interval` | `5m` | How often to check and compact |
| `targetFileSize` | `512Mi` | Target output file size |
| `minInputFiles` | `5` | Minimum files in a partition to trigger compaction |
| `minCommitInterval` | `15m` | Minimum time between commits (prevents write amplification) |
| `safemode.enabled` | `true` | Reject commits during source downtime |
| `safemode.maxDowntime` | `10m` | Max source downtime before safemode kicks in |
| `safemode.checkInterval` | `1m` | How often to check source connectivity |

### Safemode

When safemode is enabled, the maintenance worker checks whether the
source is reachable before committing a compaction. If the source has
been unreachable for longer than `maxDowntime`, the commit is rejected.
This prevents compacting away files that might be needed for a pending
CDC replay.

### Write amplification guard

The `minCommitInterval` prevents compaction from firing too frequently on
high-churn tables. If the last commit was less than `minCommitInterval`
ago, the compaction pass is skipped even if there are enough input files.

## Snapshot expiry

Iceberg retains historical snapshots for time-travel queries. Over time,
this history accumulates. Snapshot expiry removes old snapshots while
respecting your retention policy.

### Configuration

| Field | Default | Description |
| --- | --- | --- |
| `interval` | `10m` | How often to check and expire |
| `retainLast` | `1` | Always keep at least this many recent snapshots |
| `maxAge` | `168h` (7 days) | Safety window — snapshots younger than this are never expired |

### The `maxAge` safety window

`maxAge` is not just retention — it's a safety net. If your pipeline is
down for 6 hours and you need to replay from a snapshot, `maxAge` must
cover that downtime. A `maxAge` of `168h` (7 days) gives you a full week
to detect and recover from failures.

If you reduce `maxAge` below your maximum tolerable downtime, you risk
losing the ability to replay.

### Retained metadata

Each expired snapshot retains its metadata file so that the snapshot
history is never completely blank. This is a small cost (a few KB per
snapshot) that preserves auditability.

## Orphan file cleanup

Deleted data files are removed by snapshot expiry, but the physical files
on object storage may linger. Orphan cleanup scans the warehouse for files
not referenced by any snapshot and deletes them after a configurable delay.

### Configuration

| Field | Default | Description |
| --- | --- | --- |
| `interval` | `1h` | How often to scan for orphans |
| `olderThan` | `72h` | Only delete files older than this |

### Safety

The `olderThan` delay ensures that files are not deleted while they might
still be referenced by a concurrent writer or a long-running read. Three
days is conservative; reduce it only if you're confident no concurrent
access occurs.

## Maintenance events

Each maintenance run emits events visible in the dashboard and via the
`/api/v1/events` endpoint:

| Event type | What happened |
| --- | --- |
| `compaction` | Files merged, bytes reduced |
| `snapshot_expiry` | Old snapshots removed |
| `orphan_cleanup` | Unreferenced files deleted |

Example event:

```json
{
  "ts": "2026-09-17T11:05:32.123456789Z",
  "type": "compaction",
  "worker": "orders-demo-bronze.orders-0",
  "table": "bronze.orders",
  "message": "compaction",
  "fields": {
    "files_removed": 48,
    "files_added": 6
  }
}
```

## Prometheus metrics

Maintenance exposes per-operation metrics:

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `urutau_maintenance_runs_total` | Counter | `pipeline`, `table`, `operation` | Total maintenance runs |
| `urutau_maintenance_files_removed_total` | Counter | `pipeline`, `table`, `operation` | Files removed |
| `urutau_maintenance_files_added_total` | Counter | `pipeline`, `table`, `operation` | Files added |
| `urutau_maintenance_bytes_before_total` | Counter | `pipeline`, `table`, `operation` | Bytes before operation |
| `urutau_maintenance_bytes_after_total` | Counter | `pipeline`, `table`, `operation` | Bytes after operation |
| `urutau_maintenance_duration_seconds` | Histogram | `pipeline`, `table`, `operation` | Operation duration |

See [Monitoring](monitoring.md#metrics) for scrape configuration.

## Conflict handling

Maintenance operations acquire a partition lock before writing. If another
worker (CDC or maintenance) holds the lock, the maintenance pass is
skipped for that partition. This is safe — the next interval will pick it
up. No data is lost, no commits are retried.

The lock is per-partition, not per-table, so compacting partition A does
not block writing to partition B.

## Example: full maintenance spec

```yaml
sink:
  type: iceberg+rest
  uri: http://catalog:8181
  warehouse: s3://warehouse
  tables:
    - source: shop.orders
      target: bronze.orders
      writeMode: upsert
      commitMode: atomic
    - source: shop.products
      target: bronze.products
      writeMode: append

  maintenance:
    enabled: true
    compaction:
      interval: 5m
      targetFileSize: 512Mi
      minInputFiles: 5
      minCommitInterval: 15m
    snapshotExpiry:
      interval: 10m
      retainLast: 2
      maxAge: 336h
    orphanCleanup:
      interval: 2h
      olderThan: 96h
```

## Example: full-dump append-only table

```yaml
sink:
  type: iceberg+rest
  uri: http://catalog:8181
  warehouse: s3://warehouse
  tables:
    - source: shop.big_table
      target: bronze.big_table
      writeMode: append

  maintenance:
    enabled: true
    compaction:
      interval: 15m
      targetFileSize: 1Gi
      minInputFiles: 10
```

## Troubleshooting

**Compaction isn't running**

- Check `maintenance.enabled: true` is set
- Check `minInputFiles` — if your table has fewer files than this
  threshold, compaction won't trigger
- Check `minCommitInterval` — if the last commit was recent, the pass
  is skipped
- Check events for errors (catalog connectivity, permissions)

**Orphan files accumulating**

- Increase `orphanCleanup.interval` if scans are too slow
- Decrease `olderThan` if you're confident about no concurrent access
- Check Prometheus metrics for `urutau_maintenance_runs_total` to verify
  the operation is running

**Snapshot expiry not firing**

- `retainLast` ensures at least N snapshots are always kept — if your
  table has fewer snapshots than this, nothing is expired
- `maxAge` prevents expiring recent snapshots — if all snapshots are
  younger than `maxAge`, nothing is expired

**Maintenance worker crashing**

- Check coordinator logs for errors
- Common causes: catalog connectivity, S3 permissions, Iceberg schema
  mismatches
- The supervisor resets the worker automatically; check the epoch counter
  in the dashboard
