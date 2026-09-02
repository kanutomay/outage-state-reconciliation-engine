# Test Scenarios

The original delivery included an internal acceptance procedure covering the CREATE → UPDATE → CLOSE lifecycle and negative/error paths. The retained execution record documents 18 executed scenarios and 18 passes; a separate organizational unit conducted additional testing whose results are not part of this portfolio. Those internal workbooks are intentionally excluded because they contain real endpoints, operating-company names, infrastructure identifiers, and operational evidence.

`test_public_contract.py` is a sanitized, dependency-free `unittest` suite for the reconstructed public samples. It verifies API-boundary behavior, deterministic FIFO deduplication, validation blocking, correlation-ID handling, and cleanup ticket grouping without requiring AWS credentials or publishing internal test data.

Run it from the repository root:

```bash
python -m unittest discover -s tests -v
```

The broader catalog below remains valuable for future integration coverage. Some scenarios require downstream processor and data-management components that are intentionally not included in this repository.

Representative scenarios worth automating first, in priority order:

1. **CREATE happy path** - new ticket, valid category, no node conflicts → `200`.
2. **CREATE duplicate ticket** → `409 TICKET_ALREADY_EXISTS`.
3. **CREATE node conflict** - node already on an open ticket → `409 NODE_ALREADY_IN_OUTAGE`.
4. **UPDATE on unknown ticket** → `404 TICKET_NOT_FOUND`.
5. **UPDATE on closed ticket** → `422 INVALID_TICKET_STATE`.
6. **CLOSE on unknown ticket** → `404 TICKET_NOT_FOUND`.
7. **CLOSE on already-closed ticket**, `CLOSE_IDEMPOTENT=true` → `200` (idempotent success).
8. **Automated Detection → Hybrid promotion** - a manually-reported (OTS) ticket arrives for a node with an open Automated-Detection ticket → ticket promoted, not duplicated.
9. **Manual/Hybrid duplicate rejection** - an automated-detection trigger arrives for a node already on a Manual or Hybrid ticket → rejected.
10. **CLEANUP submitted via the public API** → `400 Bad Request` - CLEANUP is not an accepted intent on the entry endpoint, regardless of API key validity (see [Known Limitations L1](../docs/known-limitations.md)).
11. **CLEANUP via the scheduled path** - a ticket older than the SLA threshold gets a CLEANUP message published directly onto the shared internal lifecycle queue, bypassing pre-queue validation, and is closed with `reason=STALE_TICKET_72H`.
12. **Retried CREATE (identical payload) within the FIFO dedup window** - second send with the same ticket/intent/node-list should collapse into the first message rather than being processed twice; verifies the content-derived deduplication ID actually works, as opposed to a per-send random ID.
13. **Validation backend unavailable** - DynamoDB query raises an error during validation → `503 VALIDATION_SERVICE_UNAVAILABLE`, request rejected rather than silently allowed through.
14. **Lifecycle ordering across intents, once messages reach the queue** - a CREATE immediately followed by an UPDATE for the same ticket both land on the same queue and `MessageGroupId=ticket_id`; the processor must observe them in send order regardless of relative processing speed. Regression test for the release-review finding that separate per-intent queues never actually guaranteed this. This is queue-ordering only - it says nothing about the pre-queue validation race in scenario 24 below, which happens before either message reaches the queue.
15. **CREATE duplicate ticket under a different node set** - a ticket ID already exists, but the retried/duplicate CREATE names nodes the original ticket never touched → still `409 TICKET_ALREADY_EXISTS`, not a false `200`. Regression test for the ticket-uniqueness check that used to rely on node overlap alone.
16. **Partial DynamoDB failure during a multi-node validation call** - some node lookups in the batch succeed (and find no conflict) while at least one errors → `503 VALIDATION_SERVICE_UNAVAILABLE`, not a `200` that silently treated the failed node as conflict-free. Regression test distinguishing this from scenario 13 (a *total* backend failure), since the two used to be handled differently.
17. **CREATE/UPDATE with more than 10 nodes** → `400 Bad Request`, rejected before validation runs. Exactly 10 nodes is the boundary and must still succeed.
18. **CREATE with no `Devices`** → `400 Bad Request`, rejected before validation runs rather than accepted as automatically valid. Regression test for the bypass that skipped duplicate/conflict checking entirely on a nodeless CREATE.
19. **Request missing `OpCo`** (any intent) → `400 Bad Request`. Regression test for a documented-required field that was previously never checked.
20. **CREATE/UPDATE/CLOSE with one malformed node ID mixed into an otherwise valid `Devices` list** → `400 Bad Request` for the whole request, not a `200` that silently validated only the well-formed nodes.
21. **Two different requests sharing `ticket_id`/`intent`/nodes but different `OpCo` or `category`, sent within the FIFO dedup window** → both processed as distinct messages, not collapsed into one. Regression test for the dedup hash that used to omit `OpCo` and `category`.
22. **`correlation_id` supplied only in the request body, or only via a lowercase `x-correlation-id` header** → the caller-supplied value is used in the response and downstream messages, not a freshly generated one. Regression test for the case-sensitive, header-only lookup that ignored the documented body field.
23. **Cleanup run with more than 500 stale tickets** → every stale ticket across all pages is queued for closure, not just the first 500. Regression test for the missing `NextToken` pagination in `find_stale_tickets()`.
24. **A ticket that exists but only shows up past the first page of the ticket-ID-scoped scan** → `check_ticket_exists()` must follow `NextToken` and find it, not report `404`/allow a duplicate `CREATE` just because the first `Limit=200` page came back empty. Regression test for the peer-review finding that `Limit` bounds items evaluated per page, not items matched - see [Known Limitations L8](../docs/known-limitations.md).
25. **CREATE immediately followed by UPDATE or CLOSE for the same ticket, before the processor has written the ticket** → the second request's synchronous existence check runs ahead of the still-in-flight write and gets `404 TICKET_NOT_FOUND`, with a `blocked_reason.note` telling the caller to retry shortly rather than a bare 404. This is documented, expected behavior, not a bug to "fix" in this test - see [Known Limitations L10](../docs/known-limitations.md) for why, and scenario 14 above for the queue-ordering guarantee this does *not* contradict.
26. **Same `ticket_id`/node, different `OpCo`** → a CREATE for `ticket_id=774471`/`node=DEMO-NODE-001` under one OpCo must not be blocked as a duplicate or node-conflict by an unrelated ticket carrying the same `ticket_id` or node under a *different* OpCo; the same combination under the *same* OpCo still correctly conflicts. Regression test for the peer-review finding that `OpCo` was trusted in the request but never applied to any DynamoDB key or query condition - see [Known Limitations L9](../docs/known-limitations.md).
27. **Cleanup grouping a stale ticket whose ID itself contains an underscore** (e.g. `INC_2026_001`) → grouped and published for closure as `INC_2026_001`, not truncated to `INC`. Regression test for `group_nodes_by_ticket()` splitting on the first underscore instead of stripping the known `_{node_id}` suffix - see [Known Limitations](../docs/known-limitations.md).
28. **Retried CREATE that collides with SQS FIFO's content-based dedup** → the second call still gets a synchronous `200` with its own freshly generated `correlation_id`, but that ID is documented (not silently wrong) as one that won't appear in downstream logs, since only the original message's ID reaches the processor. Not a bug this test "fixes" - a regression/documentation test confirming the behavior matches [Known Limitations L11](../docs/known-limitations.md) rather than silently drifting from it.

A future integration suite using local AWS service doubles could cover more of scenarios 1-7 and 10-28 without needing a live AWS environment. Secrets Manager is not part of the three included handlers; the API-key check belongs to the upstream router Lambda, outside this repository's representative samples (see [docs/api-reference.md](../docs/api-reference.md)).
