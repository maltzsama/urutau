---
sidebar_position: 3
---

# Enriching a stream against a reference table

CDC gives you the source table's own columns. Often you need one more —
a customer's tier, a product's category — that lives in a different,
mostly-static table. `enrich` joins your event stream against that table
in memory, with no per-event query and no join engine: the whole
reference is loaded once, held as a broadcast map, and refreshed on a
timer.

This is a worked example. For the full field-by-field grammar, see
[Enrichment](../reference/enrichment.md).

## The scenario

You're replicating `shop.orders`. Every order carries a `customer_id`,
but you want the customer's `name` and `tier` on the row too, without
adding a join to every downstream query.

```yaml title="Enrichment pipeline"
pipeline: orders-enriched
source:
  kind: mysql
  uri: mysql://repl:replpass@127.0.0.1:3306/shop
sink:
  uri: http://localhost:8181/api/catalog
  namespace: raw
tables:
  - source: shop.orders
    target: raw.orders
    primaryKey: [id]
    enrich:
      - table: customers
        source:
          uri: "mysql://repl:replpass@127.0.0.1:3306/shop"
          query: "SELECT id, name, tier FROM customers"
        on: {customer_id: id}
        select: [name, tier]
        joinType: left
        as: {"customers.name": "customer_name"}
```

Every row in `raw.orders` now also has `customer_name` and
`customers.tier` (unrenamed columns keep the `{table}.` prefix
automatically, so a second reference can't collide with this one).

## Reading the pieces

- **`table: customers`** names this reference — shows up in diagnostics,
  and is the prefix every unrenamed output column gets.
- **`source.uri` / `source.query`** is a plain SQL connection and a full
  read of the reference — not CDC'd, not filtered per event. Whatever
  this query returns is the entire reference image until the next
  refresh.
- **`on: {customer_id: id}`** — one pair, event column on the left, mapped
  to the reference column on the right. Exactly one pair; multi-column
  joins aren't supported yet.
- **`select: [name, tier]`** — explicit projection. Nothing outside this
  list reaches the sink, even if the query returns more columns.
  `select: ["*"]` takes every column the query returns.
- **`joinType: left`** — a `customer_id` with no matching customer still
  passes through, with `customer_name`/`customers.tier` set to NULL. See
  below for the other four values.
- **`as: {"customers.name": "customer_name"}`** — renames one output
  column. The key must be the table-prefixed form (`customers.name`, not
  `name`) and must name a column in `select`.

## Choosing a `joinType`

There's no default — every reference must declare one:

| `joinType` | On a miss | Reference columns in output |
|---|---|---|
| `left` (or `left outer`) | row passes through | yes, NULL |
| `inner` | row is dropped | yes |
| `left semi` | row passes only if it matched | **no** (existence check) |
| `left anti` | row passes only if it did NOT match | **no** (existence check) |

`left semi`/`left anti` are existence checks, not enrichments — you can't
set `select` on them (there's nothing to project). Use `left semi` for
"only orders from a known customer," `left anti` for "only orders from an
unknown one." A delete event always passes through regardless of
`joinType` — the lookup is skipped for deletes entirely.

## Sizing the reference: `maxRows`

The reference is held **entirely in worker RAM** — there's no bound by
default beyond `maxRows`, which defaults to 5,000,000 rows. If your query
returns more than that, the pipeline fails loudly at load time rather
than silently ballooning memory:

```yaml title="maxRows guardrail"
enrich:
  - table: customers
    source: {uri: "...", query: "SELECT id, name, tier FROM customers"}
    on: {customer_id: id}
    select: [name, tier]
    joinType: left
    maxRows: 200000
```

Set this to whatever your actual reference size is — it's a guardrail
against a query that unexpectedly returns the whole database, not a
tuning knob to raise casually.

## Keeping the reference fresh

```yaml title="Refresh settings"
    refresh: 5m        # default
    onColdStart: buffer # default
```

`refresh` re-runs the reference query on this interval and atomically
swaps in the new image — an event mid-flight finishes against the old
one, never a half-updated map. `onColdStart` decides what happens to a
batch that arrives before the *first* load completes: `buffer` (default)
and `pass` currently behave the same — the batch passes through as a
miss, following the join type's miss behavior. `drop` discards the whole
batch instead.

## What you don't get

Enrichment is a snapshot join, not CDC: if `customers.tier` changes after
an order was enriched, that order's row in `raw.orders` does not
retroactively update. Only new events joined after the next `refresh` see
the new value. Enriched columns are also not reproducible by replaying
the source from scratch — the reference at replay time may differ from
the reference at original ingest time. The source's own columns and
position stay fully deterministic either way; only the enriched columns
carry this caveat.

For everything else — multiple references on one table, wildcard
`select: ["*"]`, the exact validation rules — see
[Enrichment](../reference/enrichment.md).
