---
sidebar_position: 10
---

# Delete modes: deletion vectors against equality deletes

The Iceberg sink removes a row's old version in one of two ways
(`sink.deleteMode`): an **equality delete** on the primary key, cheap to
write and applied by every reader until a compaction, or a **deletion
vector** (`positional`), which marks the old row's position and so has to
find it first. This page records what each costs on the write side and on the
read side, measured with the [benchmark harness](benchmark-harness.md)
(issue #750).

## What was run

Eight runs: both modes over four table layouts. One table (`events`,
composite key `(tenant_id, event_id)`, 64 tenants), 200,000 seeded rows, an
8-minute live window at a mean of 300 mutations/s in transactions of up to
200 rows, 15% inserts, 75% updates, 10% deletes, payloads up to 1 KiB, one
worker, no maintenance. The configurations are in
`test/e2e/pods/bench/delete-mode/`.

| layout | partitioning | updates |
|--------|--------------|---------|
| unpartitioned | none | uniform over the table |
| bucket | `bucket(16, event_id)` | uniform |
| tenant | `identity(tenant_id)` | uniform |
| tenant, hot | `identity(tenant_id)` | 90% on 4 of the 64 tenants |

Environment: Urutau at `0223d42` (production image), one minikube node (16
CPU, 46 GiB) with MySQL 8.4.11, RustFS, Polaris and Trino 483 on the same
node. The numbers compare the modes on this node; they are not a
characterization of Urutau at scale. Another project's Pods ran on the node
during some of the runs.

## Write side

| layout | mode | converged | end-to-end latency p50 / p99 | worker commits | mean commit | data files read per commit (mean / max) | worker memory, max |
|--------|------|-----------|------------------------------|----------------|-------------|------------------------------------------|--------------------|
| unpartitioned | equality | yes, 33 s after the window | 4.1 s / 5.7 s | 253 | 0.39 s | — | 142 MiB |
| unpartitioned | positional | yes, 55 s | 21.2 s / 46.7 s | 158 | 3.11 s | 78 / 156 | 133 MiB |
| bucket | equality | yes, 69 s | 10.2 s / 20.1 s | 249 | 1.02 s | — | 73 MiB |
| bucket | positional | yes, 724 s | 336 s / 688 s | 66 | 17.7 s | 494 / 941 | 188 MiB |
| tenant | equality | yes, 103 s | 21.4 s / 51.4 s | 161 | 2.94 s | — | 159 MiB |
| tenant | positional | **no** (15-minute settle) | 368 s / 1,096 s for what did arrive; 8,150 transactions never visible | 33 | 87 s and growing | 958 / 1,979 | 257 MiB |
| tenant, hot | equality | yes, 80 s | 22.7 s / 43.6 s | 150 | 3.32 s | — | 100 MiB |
| tenant, hot | positional | **no** | 574 s / 1,135 s for what did arrive; 10,424 never visible | 20 | 180 s and growing | 415 / 972 | 226 MiB |

What the numbers say:

- **A positional commit reads every data file of the table.** The mean of
  files read per commit is half the final file count in every layout: each
  commit read all the files that existed. The scan plan's filter is the range
  of the batch's first key column, and a batch of uniform updates spans the
  whole range. Concentrating the updates on four tenants changed nothing: the
  other 10% still span every tenant.
- **Partitioning multiplies the cost.** Every commit adds a data file per
  partition it touches, so the file count grows 16 or 64 times faster, and
  the next commit reads them all. Unpartitioned, a positional commit costs 8
  times an equality one and the pipeline keeps up. With 16 buckets it costs
  17 times and the sink runs minutes behind. With 64 tenant partitions the
  commits take minutes and the pipeline does not catch up.
- **The time is spent waiting on the object store, not computing.** The
  positional worker used at most 0.55 CPU; the files are read one after
  another.
- **Memory is not the constraint.** The positional worker stayed under
  260 MiB.

## Read side

A full scan through Trino (`SELECT count(*), sum(rev)`), median of three,
once the run had settled; then `ALTER TABLE … EXECUTE optimize`, and the
same scan again.

| layout | mode | data / delete files before | scan before | compaction | data / delete files after | scan after |
|--------|------|----------------------------|-------------|------------|---------------------------|------------|
| unpartitioned | equality | 252 / 252 | 1.40 s | 8.4 s | 1 / 1 | 0.10 s |
| unpartitioned | positional | 157 / 152 | 0.70 s | 6.8 s | 1 / 0 | 0.12 s |
| bucket | equality | 3,942 / 3,953 | 8.19 s | 19.1 s | 16 / 16 | 0.18 s |
| bucket | positional | 957 / 871 | 1.04 s | 6.7 s | 16 / 0 | 0.18 s |
| tenant | equality | 8,696 / 8,755 | 12.16 s | 18.8 s | 64 / 65 | 0.20 s |
| tenant, hot | equality | 7,477 / 7,569 | 8.97 s | 15.8 s | 64 / 20 | 0.19 s |

The two positional runs on the tenant layout did not converge and were not
scanned.

- Before a compaction, the positional tables scan 2 to 8 times faster. Part
  of that is fewer files: the positional runs made fewer, larger commits
  because each one took longer, so the two sides do not hold the same number
  of files.
- After a compaction the modes are indistinguishable.

## After #864

The write-side numbers above were taken at `0223d42`. They showed where a
positional commit's time went, and three changes followed (#864): the plan
filter covers every key column and lists up to 200 distinct values one by
one, so a small batch reads only the partitions and files its keys can be
in; the candidate data files are read sixteen at a time; and the deletion
vectors a commit replaces are read one Puffin file at a time instead of one
vector at a time. That last one was the larger cost: a commit replacing 1,573
vectors spent 36 s decoding the same few Puffin footers.

The four positional runs again, same configurations, at `ddd3cc4`:

| layout | converged | end-to-end latency p50 / p99 | mean commit (before) | data files read per commit (mean / max) | worker memory, max | scan before compaction |
|--------|-----------|------------------------------|----------------------|------------------------------------------|--------------------|------------------------|
| unpartitioned | yes, 30 s after the window | 3.9 s / 10.4 s | 1.42 s (3.11 s) | 122 / 243 | 157 MiB | 0.99 s |
| bucket | yes, 80 s | 57.9 s / 83.7 s | 4.0 s (17.7 s) | 773 / 1,760 | 252 MiB | 1.29 s |
| tenant | yes, 172 s | 66.6 s / 141 s | 7.36 s (87 s, growing) | 1,314 / 3,679 | 287 MiB | 2.59 s |
| tenant, hot | yes, 229 s | 86.8 s / 237 s | 7.22 s (180 s, growing) | 1,355 / 3,972 | 274 MiB | 2.80 s |

Every layout now converges with Iceberg equal to MySQL. Unpartitioned, the
end-to-end latency matches equality mode (3.9 s against 4.1 s) at 3.6 times
its commit cost. Partitioned, a positional commit still costs 2.5 to 4 times
an equality one and the sink runs about a minute behind under this load,
where equality runs 10 to 20 s behind.

What did not change: a batch of thousands of keys spread over the table
still has every data file as a candidate. The files read per commit are
still half the final file count; they are only read faster. The cost of a
positional commit stays proportional to the number of data files, which is
what a compaction bounds and what the index of #754 would remove.

With the deletes already applied by position, the scan before a compaction
takes 1.0 to 2.8 s on tables where equality deletes take 1.4 to 12.2 s.

## Recommendations

**A persistent key → position index (#754).** The condition for opening it
is met by the first set of numbers, and the scan has since been made as cheap
as a scan gets (#864): pruned by every key column, read concurrently. What
remains is structural. A positional commit of a batch spread over the table
reads the key columns of every data file, so its cost grows with the file
count: 1.4 s at 240 files, 7 s at 3,700. Maintenance compacting the table
bounds that; the index removes it. Whether the index is worth building
depends on how far behind a partitioned table may run: about a minute under
this load without it.

**`bucket(N, pk)` as a table layout.** Not on these numbers. A batch of
thousands of uniformly spread keys touches every bucket, so bucketing prunes
nothing and multiplies the data files: the bucketed table committed in 4.0 s
where the unpartitioned one took 1.4 s.

**Positional deletes as the default (#755).** Not yet. On an unpartitioned
table the mode now keeps the latency of equality deletes and scans faster
before a compaction, at 3.6 times the commit cost. On a partitioned table it
runs a minute behind. Equality stays the default; the positional mode stays
opt-in and experimental.
