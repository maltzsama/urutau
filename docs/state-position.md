# Position: where the checkpoint lives

> Answers the structural audit finding: *"the arbitration rule is not
> written anywhere"* — the arbitration between the sink and the
> bbolt store. The authoritative statement lives on the package doc
> (`internal/state/store.go`, "ROLE AND ARBITRATION"); this page makes it
> findable and records the decision.

## The thesis

**The position lives in the sink, atomic with the data** (design §17.3).
Iceberg commits the checkpoint in the same transaction as the data files —
there is no second place where "where did we get to" can disagree with
"what is actually there".

## The one exception

External plugin sinks (contract §9) cannot persist a position themselves —
a generic sink has no transactional metadata table to write the offset
into, and the contract explicitly presupposes that Urutau commits offsets
on the plugin's behalf. For THOSE pipelines, `internal/state` (bbolt) is
the position store.

The store is therefore the **exception**, scoped by sink capability — not
a parallel checkpoint system. Its durability decision (fsync on every
commit, NoSync forbidden) and its layout are documented in the package
doc.

## Arbitration rule

If, for the same pipeline/table, both places hold a position:

1. **Sink position wins.** It was written atomically with the data — the
   only one that cannot diverge from what the sink actually contains.
2. **The bbolt store is authoritative only when the sink has no position
   capability** (external plugin sinks).
3. **On resume:** consult the sink first; the store is the fallback, and
   is overwritten on the next commit.
4. **The two are never merged.** One wins per table.

## Status

- Implemented: the store, its durability contract, its layout, and this
  arbitration rule (documented at `internal/state/store.go`).
- Not yet wired: no production code path imports `internal/state` — the
  plugin-sink committer that consumes it does not exist yet. The package
  stays in tree as the designed landing spot for contract §9 offsets; the
  audit's risk note stands: importing it WITHOUT this rule would create a
  second source of truth. The rule is above.
