---
sidebar_position: 11
---

# History server

The live dashboard shows a **running** pipeline. The history server answers a
different question: what did a **terminated** run actually do? It serves a
read-only API over the durable [audit trail](reliability.md#audit-trail-eventlog)
— the JSONL trail in S3 — with no live state and nothing to push, so it is
plain request/response, not Server-Sent Events.

It is a standalone binary, `urutau-history-server`, separate from the
coordinator and the operator. Discovery is **S3-only**: it lists the trail's
prefixes and never queries the Kubernetes API, so it needs no cluster RBAC —
just read-only object-storage credentials.

## Prerequisite: a trail exists

The history server reads what the coordinator wrote. An operator-managed
pipeline writes nothing unless you set `spec.coordinator.eventlog`:

```yaml
spec:
  coordinator:
    eventlog:
      bucket: my-trails
      rootPrefix: urutau
```

That writes the trail under the shared key convention
`s3://<bucket>/<prefix>/<pipeline>/run-<id>/events-NNNNNN.jsonl`, which is
exactly what the server lists. A pipeline with no `eventlog` has no history.

## Run it

```sh
urutau-history-server serve \
  --root s3://my-trails/urutau \
  --listen :8080
```

S3 credentials and region come from the standard `AWS_*` environment (or
`--region`/`--endpoint`/`--access-key`/`--secret-key` for a MinIO-style
store). Give it **read-only** access — it never writes.

## API

| Route | Returns |
| --- | --- |
| `GET /api/v1/pipelines` | `{"pipelines":[{"name":"shop"}]}` |
| `GET /api/v1/pipelines/{name}/runs` | `{"runs":[{"id":"…","started":"…"}]}` |
| `GET /api/v1/pipelines/{name}/runs/{runId}/events?cursor=…` | `{"events":[…],"nextCursor":"…"}` |

Events are paginated: pass the returned `nextCursor` to fetch the next page
(`--page-limit` sets the page size, default 1000). Each event is
`{"timestamp","runId","kind","fields"}` — `fields` carries the event's
free-form payload (a `commit` event's table, a `worker_reset` event's reason,
and so on).

```sh
curl -s localhost:8080/api/v1/pipelines | jq
curl -s localhost:8080/api/v1/pipelines/shop/runs | jq
curl -s 'localhost:8080/api/v1/pipelines/shop/runs/<runId>/events' | jq
```

The server reads the trail directly and does not cache it, so a run still in
progress is readable too — its latest events are simply the ones written so
far.
