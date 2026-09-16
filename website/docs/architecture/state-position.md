---
sidebar_position: 3
---

# Position: where the checkpoint lives

> Answers the structural audit finding: *"the arbitration rule is not
> written anywhere"*. This page is the authoritative statement of the rule
> and of the one case the sink cannot cover.

## The thesis

**The position lives in the sink, atomic with the data** (design §17.3).
Iceberg commits the checkpoint in the same transaction as the data files —
there is no second place where "where did we get to" can disagree with
"what is actually there".

## The one exception

External plugin sinks (contract §9) cannot persist a position themselves —
a generic sink has no transactional metadata table to write the offset
into, and the contract explicitly presupposes that Urutau commits offsets
on the plugin's behalf. For those pipelines the position needs a store
outside the sink.

That store is **not implemented**. It was sketched once as a bbolt package
(`internal/state`) but no production path ever imported it, so it was
removed rather than kept as dead code. See
[Roadmap: plugin-sink position store](../reference/roadmap.md#registered-for-v2-not-v1-gaps).

## Arbitration rule

If, for the same pipeline/table, both a sink position and an out-of-sink
store hold a position, the rule is:

1. **Sink position wins.** It was written atomically with the data — the
   only one that cannot diverge from what the sink actually contains.
2. **The store is authoritative only when the sink has no position
   capability** (external plugin sinks).
3. **On resume:** consult the sink first; the store is the fallback, and is
   overwritten on the next commit.
4. **The two are never merged.** One wins per table.

This rule is the design constraint any future store must honor: importing a
second source of truth *without* it would create exactly the divergence the
position-in-sink model exists to prevent.
