---
sidebar_position: 4
---

# Enrichment

Broadcast hash join against small reference tables (`internal/enrich`).
The reference is read whole into worker RAM; every event matches in O(1)
against the map; a periodic full re-read swaps the map atomically (an
in-flight event finishes against the old image). No lookup per event, no
shuffle, no windowed state.

## Rules

- **Projection is explicit.** `select` is required and lists exactly the
  reference columns the event receives — nothing unselected ever lands in
  the sink. `select: ["*"]` is the wildcard sugar for "take everything";
  it must be the only entry.
- **Column namespacing follows Spark DataFrame semantics.** An unrenamed
  column is auto-prefixed `{table}.{column}`, so two references injecting
  a same-named column never silently collide. `as` renames at load time
  and overrides the prefix.
- **`joinType` is required** and accepts five values: `left` (alias
  `left outer`), `inner`, `left semi`, `left anti`. `left`/`left outer`
  pass a miss with the reference columns NULL; `inner` drops a miss;
  `left semi` keeps a hit **without** the reference columns in the
  output; `left anti` keeps a miss **without** the reference columns.
  `select` is rejected as a spec error on `left semi`/`left anti` —
  neither emits reference columns. A delete always survives every join
  type, bypassing the lookup entirely.
- **`onColdStart` is decided per BATCH, not per event.** Before the
  reference's first load completes: `drop` drops the whole batch; `buffer`
  and `pass` are currently identical — every row in the batch passes as a
  miss (reference columns NULL, following the join type). There is no
  per-event buffer or drain queue; `bufferLimits` is still validated as
  spec grammar but tunes nothing today.
- **A wildcard reference's real columns are known before boot completes**,
  not just after the first refresh — see
  [Semantics](semantics#wildcard-select-no-schema-drift-issue-56).
- **Point-in-time, and it says so.** Enriched columns are not reproducible
  by replay (the reference is a snapshot, not CDC); source columns and
  position stay deterministic either way.

Full grammar detail, the boot-time join-key type check, duplicate-key
rejection, and cold-start semantics: [Semantics](semantics#enrich-columnar-broadcast-join).

## Examples

Every example below is a complete, valid pipeline spec — `source`/`sink`
are required on the reference too (`spec.EnrichSource`), and event columns
come from the source table's own schema (introspected for SQL sources),
not a `columns:` list on `tables[]`.

**Single reference:**
```yaml
pipeline: orders-enriched
source:
  kind: mysql
  uri: mysql://repl:replpass@127.0.0.1:3306/shop
sink:
  type: iceberg+rest
  uri: http://localhost:8181/api/catalog
  warehouse: quickstart_catalog
  namespace: bronze
  clientId: root
  clientSecret: s3cr3t
  scope: PRINCIPAL_ROLE:ALL

tables:
  - source: shop.orders
    target: bronze.orders
    primaryKey: [id]
    writeMode: upsert
    createIfNotExists: true
    enrich:
      - table: customers
        source:
          uri: mysql://repl:replpass@127.0.0.1:3306/shop
          query: SELECT id, name, tier FROM customers
        on: {customer_id: id}
        select: [name, tier]
        joinType: left
```
An order row `{id: 1, customer_id: 42, amount: 100}` lands as
`{id: 1, customer_id: 42, amount: 100, customers.name: "Ana", customers.tier: "gold"}`.
A `customer_id` with no matching row lands with `customers.name` and
`customers.tier` both NULL (left join, not dropped).

**Multiple references** (no collision — each gets its own prefix):
```yaml
enrich:
  - table: customers
    source: {uri: "mysql://repl:replpass@127.0.0.1:3306/shop", query: "SELECT id, name FROM customers"}
    on: {customer_id: id}
    select: [name]
    joinType: left
  - table: products
    source: {uri: "mysql://repl:replpass@127.0.0.1:3306/shop", query: "SELECT id, name, category FROM products"}
    on: {product_id: id}
    select: [name, category]
    joinType: left
```
→ `{..., customers.name: "Ana", products.name: "Laptop", products.category: "electronics"}`

**Renaming with `as`:**
```yaml
enrich:
  - table: customers
    source: {uri: "mysql://repl:replpass@127.0.0.1:3306/shop", query: "SELECT id, name, tier FROM customers"}
    on: {customer_id: id}
    select: [name, tier]
    joinType: left
    as: {"customers.name": "client_name"}
```
→ `{..., client_name: "Ana", customers.tier: "gold"}` — `name` renamed,
`tier` keeps its prefix. `as` keys must be the **table-prefixed** column
name (`customers.name`, not `name`) and must name a column in `select`.

:::caution Blocked by [issue #65](https://github.com/maltzsama/urutau/issues/65)
`spec.Validate()` currently rejects this exact example — it checks `as`
keys against the *unprefixed* column name instead of the prefixed one
`internal/enrich` actually requires at boot. Right now neither form of
`as` passes end to end: the unprefixed form passes `Validate()` but fails
later at `enrich.New()`; the prefixed form shown above is what the join
itself requires but `Validate()` rejects it first. The prefixed form is
correct — document it here so it's ready the moment #65 lands; until
then, a pipeline using `as` will not boot.
:::

`joinType` is required on every reference — there is no default miss
policy; see [Rules](#rules) above for the five accepted values.
