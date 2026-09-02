# API Reference

## Endpoint

```
POST /{stage}/ots_resource
```

Authenticated via an `X-Api-Key` header, validated against a value stored in AWS Secrets Manager. That check happens in the upstream API-key/routing Lambda that sits in front of the entry Lambda shown in this repo (see [architecture-overview.md](architecture-overview.md) - "API Gateway layer") - it isn't one of this repo's flagship code samples, so neither its code nor the Secrets Manager resource it reads from appears in [infra/cloudformation/outage-pipeline-stack.yaml](../infra/cloudformation/outage-pipeline-stack.yaml). All requests and responses are JSON.

> **CLEANUP is not part of this API.** It's an internal-only operation produced by a scheduled Lambda that publishes directly onto an internal queue - there is no request shape for it here because there is no way to submit it externally, with or without an API key. See [Known Limitations (L1)](known-limitations.md).

## Request envelope

| Field | Type | Required | Notes |
|---|---|---|---|
| `OpCo` | string | yes | Operating-company / tenant code. |
| `intent` | string | yes | One of `CREATE`, `UPDATE`, `CLOSE`. |
| `id` | string | yes | Ticket number. |
| `category` | string | CREATE only | `"HFC Access"` or `"FTTH Access"`. |
| `Devices` | string | CREATE (required) / UPDATE (optional) | Comma-separated list of affected network node IDs. At least one is required for CREATE - a nodeless CREATE can't be conflict-checked, so it's rejected. Capped at 10 nodes per request - see [Known Limitations](known-limitations.md). |
| `correlation_id` | string | no | Caller-supplied trace ID. If omitted, the `X-Correlation-Id` header is used instead (matched case-insensitively); if neither is present, one is generated. |

`OpCo` and (for CREATE) `Devices` are enforced, not just documented: a request missing either gets a `400`, not a silent pass-through - see [Known Limitations](known-limitations.md) for the two bugs this closes. `OpCo` also scopes every duplicate/conflict/existence check performed for this request, so the same `ticket_id` or node under two different `OpCo` values is never treated as a collision. It is *not* an access-control field - nothing here confirms the caller is entitled to act as the `OpCo` it supplies. See [Known Limitations (L9)](known-limitations.md).

## Response envelope

Every endpoint - success or failure - returns the same shape:

```json
{
  "message": "Ticket queued successfully",
  "status": "SUCCESS",
  "timestamp": "2026-01-15T09:32:10.421-05:00",
  "ticket_id": "774471",
  "correlation_id": "b3b7e6b0-1a2b-4c3d-9e8f-0123456789ab"
}
```

A rejected request includes `error_type` and `error_details` describing exactly what conflicted:

```json
{
  "message": "1 node(s) already have open tickets",
  "status": "CONFLICT",
  "timestamp": "2026-01-15T09:32:10.421-05:00",
  "ticket_id": "774471",
  "correlation_id": "b3b7e6b0-1a2b-4c3d-9e8f-0123456789ab",
  "error_type": "NODE_ALREADY_IN_OUTAGE",
  "error_details": {
    "conflicting_nodes": [
      {"node_id": "DEMO-NODE-001", "existing_ticket": "774400", "status": "declared"}
    ],
    "total_conflicts": 1
  }
}
```

The specific conflicting node and ticket IDs are included deliberately - an authenticated caller needs them to remediate the request (e.g. to know which node is already on which ticket). See [Known Limitations - Data handling & logging scope](known-limitations.md) for how this differs from what's written to logs or notifications.

## HTTP status codes

| Code | Meaning | When it happens |
|---|---|---|
| `200 OK` | Accepted and queued for processing | Validation passed |
| `202 Accepted` | Accepted, but not processed as requested | Shadow-mode validation would have rejected this request |
| `400 Bad Request` | Malformed payload, missing/invalid field, an unsupported intent (including `CLEANUP`), or more than 10 nodes in one request | Client error, fix and retry |
| `404 Not Found` | Ticket doesn't exist | UPDATE/CLOSE against an unknown ticket ID - or a ticket CREATEd moments ago whose write hasn't landed yet; the response's `blocked_reason.note` calls this out and the request is safe to retry. See [Known Limitations (L10)](known-limitations.md). |
| `409 Conflict` | Duplicate ticket (matched by ticket ID, regardless of node overlap), node conflict, or (optionally) already-closed ticket | CREATE duplicate, node already in outage |
| `422 Unprocessable Entity` | Ticket exists but is in an invalid state for this operation | UPDATE against a closed ticket |
| `500 Internal Server Error` | Unexpected processing failure (e.g. enqueue failure) | Escalate via CloudWatch Logs |
| `503 Service Unavailable` | Validation backend (DynamoDB) couldn't be reached | Retry shortly - the request was intentionally *not* let through unverified. See [Known Limitations (L7)](known-limitations.md). |

## Sample requests

**CREATE**

```bash
curl -X POST "https://api.example.com/prod/ots_resource" \
  -H "X-Api-Key: $OUTAGE_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "OpCo": "ACME",
    "intent": "CREATE",
    "id": "774471",
    "category": "FTTH Access",
    "Devices": "DEMO-NODE-001,DEMO-NODE-002",
    "correlation_id": "b3b7e6b0-1a2b-4c3d-9e8f-0123456789ab"
  }'
```

**UPDATE** (replaces the full node list - see [Known Limitations #L4](known-limitations.md))

```bash
curl -X POST "https://api.example.com/prod/ots_resource" \
  -H "X-Api-Key: $OUTAGE_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "OpCo": "ACME",
    "intent": "UPDATE",
    "id": "774471",
    "Devices": "DEMO-NODE-001,DEMO-NODE-002,DEMO-NODE-003"
  }'
```

**CLOSE**

```bash
curl -X POST "https://api.example.com/prod/ots_resource" \
  -H "X-Api-Key: $OUTAGE_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "OpCo": "ACME",
    "intent": "CLOSE",
    "id": "774471"
  }'
```

## Correlation IDs

Every request carries a `correlation_id` that's propagated through the entry Lambda, the SQS message body, the ticket processor, and the DB write Lambda - so a single CloudWatch Logs Insights query against `correlation_id` reconstructs a ticket's path across the log groups belonging to structured-logging components. The DB-write and notification Lambdas still carry the ID through in the message data but haven't been migrated to structured JSON logging yet (see [Known Limitations](known-limitations.md)), so reconstructing those two hops of the path currently means grepping their log groups for the ID rather than filtering on a structured field. The entry Lambda resolves it in this order: the `X-Correlation-Id` header (matched case-insensitively - client-supplied casing varies), then the `correlation_id` body field, then a generated UUID if neither is present.
