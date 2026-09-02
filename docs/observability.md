# Observability

## Structured logging

Every component that has been migrated to structured logging emits JSON with a consistent schema, so CloudWatch Logs Insights queries work uniformly across the pipeline (see [Known Limitations](known-limitations.md) for which components still use plain-text logging).

Key fields: `correlation_id`, `ticket_id`, `intent`, `processing_status`, `error_type`, `sent_to_sqs_flag`, `opco`.

## Log groups

| Component | Log group | Key signal |
|---|---|---|
| Entry / validation | `/aws/lambda/outage-entry-validation-prod` | Validation success rate, rejection reasons |
| Ticket processor | `/aws/lambda/outage-ticket-processor-prod` | Processing status, node diff results, skip reasons |
| DB write | `/aws/lambda/outage-db-write-prod` | Write outcomes, node state changes |
| Subscriber-impact lookup | `/aws/lambda/outage-impact-lookup-prod` | Lookup latency, timeout rate |
| Notifications | `/aws/lambda/outage-noc-notify-prod` | Alert delivery outcome |
| Cleanup (scheduled) | `/aws/lambda/outage-ticket-cleanup-prod` | Stale tickets found, closure results |

These are illustrative `prod` names. Replace the suffix with the target environment, and note that only the entry/validation and cleanup functions are represented in the included CloudFormation outline.

## CloudWatch Logs Insights queries

**Track a specific ticket end-to-end**
```
fields @timestamp, ticket_id, intent, processing_status, error_type, correlation_id
| filter ticket_id = "TICKET_ID"
| sort @timestamp desc
| limit 20
```

**Recent validation failures**
```
fields @timestamp, ticket_id, intent, processing_status, error_type
| filter sent_to_sqs_flag = 0
| sort @timestamp desc
| limit 50
```

**Validation success rate**
```
fields sent_to_sqs_flag
| stats count() as total, sum(sent_to_sqs_flag) as successful
| extend success_rate_pct = (successful * 100.0 / total)
```

**Error breakdown by type**
```
fields processing_status, error_type
| filter sent_to_sqs_flag = 0
| stats count() by processing_status, error_type
| sort count desc
```

**End-to-end trace by correlation ID** (chain from the query above)
```
fields @timestamp, correlation_id, @message
| filter correlation_id = "CORRELATION_ID"
| sort @timestamp asc
```

## Key metrics

**Operational**
- Request volume by intent (CREATE / UPDATE / CLOSE / CLEANUP)
- Validation success rate - target >95%
- Processing latency, P50/P95/P99
- SQS queue depth
- Lambda error rate by component

**Business**
- Tickets created / updated / closed per day
- Customer impact (subscribers affected)
- SLA compliance - % of tickets closed within 72h
- Category distribution (HFC vs. FTTH vs. Hybrid)

## Alerting thresholds

| Severity | Condition |
|---|---|
| Critical | Validation failure rate >25% |
| Critical | SQS queue depth >100 messages |
| Critical | Lambda error rate >5% |
| Critical | No CLEANUP execution in >25 hours |
| Warning | Validation success rate <90% |
| Warning | Node-update skip rate >50% |
| Warning | Subscriber-impact lookup timeout rate >10% |

## Troubleshooting workflow

1. Start from a dashboard alert or a ticket-specific complaint.
2. Pull the entry-Lambda log entry for that `ticket_id` to get its `correlation_id`.
3. Query every log group by that `correlation_id`, sorted ascending, to reconstruct the exact path the ticket took through the pipeline.
4. Cross-reference `error_type` / `processing_status` against [Known Limitations](known-limitations.md) - several apparent "bugs" are documented, intentional behavior.
5. Re-run the tracking query after a fix to confirm resolution.
