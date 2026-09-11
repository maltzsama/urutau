---
sidebar_position: 4
---

# Enrichment

Broadcast hash join against small reference tables (`internal/enrich`).
The reference is read whole into worker RAM; every event matches in O(1)
against the map; a periodic full re-read swaps the map atomically (an
in-flight event finishes against the old image). No lookup per event, no
shuffle, no windowed state.

Full grammar, cold-start semantics, and wildcard-select behavior:
[Semantics](semantics#enrich-columnar-broadcast-join).

## Rules

- **Projection is explicit.** `select` is required and lists exactly the
  reference columns the event receives — nothing unselected ever lands in
  the sink.
- **Column namespacing follows Spark DataFrame semantics.** An unrenamed
  column is auto-prefixed `{table}.{column}`, so two references injecting
  a same-named column never silently collide. `as` renames at load time
  and overrides the prefix.
- **`joinType`:** `left` (miss → NULL reference columns) or `inner` (miss
  → row dropped; `enrich_miss` metadata column, if declared, flags a left
  miss).
- **`onColdStart`:** `buffer` | `pass` | `drop` — governs events that
  arrive before the reference's first load. The buffer is bounded by count
  (`maxEvents`) and time (`maxWait`); an evacuated event follows the join
  type, so there's never a third miss policy.
- **Point-in-time, and it says so.** Enriched columns are not reproducible
  by replay (the reference is a snapshot, not CDC); source columns and
  position stay deterministic either way. `statusz` reports
  `enrichment: point-in-time` for a pipeline that declares `enrich`. A
  pipeline without it runs the identical code path it always did.

## Examples

**Single reference:**
```yaml
tables:
  - name: orders
    columns: [id, customer_id, amount]
    enrich:
      - table: customers
        select: [name, tier]
        on: {customer_id: id}
        joinType: left
```
`{id: 1, customer_id: 42, amount: 100}` →
`{id: 1, customer_id: 42, amount: 100, customers.name: "Ana", customers.tier: "gold"}`

**Multiple references** (no collision — each gets its own prefix):
```yaml
enrich:
  - table: customers
    select: [name]
    on: {customer_id: id}
  - table: products
    select: [name, category]
    on: {product_id: id}
```
→ `{..., customers.name: "Ana", products.name: "Laptop", products.category: "electronics"}`

**Renaming with `as`:**
```yaml
enrich:
  - table: customers
    select: [name, tier]
    on: {customer_id: id}
    as: {"customers.name": "client_name"}
```
→ `{..., client_name: "Ana", customers.tier: "gold"}` — `name` renamed,
`tier` keeps its prefix.
