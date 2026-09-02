# Name: ticket_cleanup_handler.py
# Component: Scheduled maintenance Lambda
# Description: Runs once daily on an EventBridge schedule. Finds tickets
#   that have been open longer than the SLA threshold, groups their nodes
#   by ticket, and publishes a CLEANUP message directly onto the internal
#   lifecycle FIFO queue for each one - the exact same queue, group ID
#   (MessageGroupId=ticket_id), and message shape a normal CLOSE goes
#   through downstream, just produced here instead of by the public entry
#   Lambda. Using the same queue as CREATE/UPDATE/CLOSE (rather than a
#   separate close-only queue) is what keeps a CLEANUP ordered after any
#   in-flight lifecycle message for that same ticket - see
#   docs/architecture-overview.md ("Why one shared lifecycle queue?").
#
# Security note: this Lambda never calls the public API. It's unreachable
# from a client request not because it lacks network access, but because
# it simply contains no HTTP/API-key code path to reach: no API key check,
# no HTTP endpoint, no handler branch that parses or accepts an inbound
# CLEANUP request. Its only outbound action is an IAM-authorized
# sqs:SendMessage to the lifecycle queue. See docs/known-limitations.md
# (L1) for the full rationale, and src/ingestion/entry_handler.py for the
# corresponding rejection on the public side.
#
# Runtime: Python 3.11 | Memory: 512 MB | Timeout: 300s
# Trigger: EventBridge Scheduler, cron(0 7 * * ? *)  # 02:00 local time, UTC-5
#
# Design decision - why CLEANUP bypasses pre-queue validation:
#   A ticket that's been open 72+ hours may have accumulated data-quality
#   issues (stale category, missing fields from the upstream system) that
#   would fail the same validation a fresh CLOSE request goes through.
#   Since this Lambda is the only thing that can ever produce a CLEANUP
#   message, and it's only ever invoked by the trusted EventBridge schedule,
#   the tradeoff is deliberate: always guarantee closure of stale tickets
#   over enforcing strict validation against them.

import os
import json
import hashlib
import logging
from datetime import datetime, timedelta, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

CUTOFF_TIME_HOURS = int(os.environ.get('CUTOFF_TIME_HOURS', '72'))
EXCEPTION_SNS_ARN = os.environ.get('EXCEPTION_SNS_ARN')
TABLE_NAME = os.environ.get('OUTAGE_TICKET_TABLE', 'outage_ticket_db')
LIFECYCLE_TICKET_QUEUE = os.environ.get('LIFECYCLE_TICKET_SQS', 'outage-lifecycle-ticket.fifo')

# Safety cap on pagination in find_stale_tickets(), not a business limit -
# guards against a single invocation running long enough to approach the
# Lambda's own timeout on an unusually large backlog. At 500 items/page,
# 20 pages is 10,000 stale tickets; any run that hits this cap logs a
# warning so it's visible in Logs Insights rather than silently dropped.
MAX_CLEANUP_PAGES = 20

sns_client = boto3.client('sns')
dynamodb_client = boto3.client('dynamodb')
sqs = boto3.resource('sqs')

tz = timezone(timedelta(hours=-5))


def _safe_error_summary(exc, limit=200):
    """Truncate exception text before it goes into logs or SNS."""
    text = f'{type(exc).__name__}: {exc}'
    return text if len(text) <= limit else text[:limit] + '...'


def find_stale_tickets(cutoff_iso):
    """Parameterized PartiQL scan for open tickets created before the
    cutoff timestamp. cutoff_iso is server-computed (not caller input), but
    it's still passed as a bind parameter rather than interpolated -
    consistent with the pattern in src/validation/prequeue_validation.py.

    Paginates through every matching page via NextToken rather than
    reading only the first one. An earlier version made a single
    execute_statement call with Limit=500 and returned whatever came back
    - on a day with more than 500 stale tickets, everything past the first
    page was silently dropped, not just deferred: DynamoDB doesn't
    guarantee scan order lines up with Ticket_Creation_Date, so a later
    run's first 500 aren't reliably "the ones missed last time" either.
    Bounded by MAX_CLEANUP_PAGES as a Lambda-timeout safety net, not a
    business limit - see that constant's comment."""
    query = f'''
        SELECT "Ticket_Number", "Node_ID", "Ticket_Creation_Date"
        FROM "{TABLE_NAME}"
        WHERE "Ticket_Status" IN ('pending', 'declared')
          AND "Ticket_Creation_Date" < ?
    '''
    items = []
    next_token = None
    pages_fetched = 0

    while True:
        kwargs = {'Statement': query, 'Parameters': [{'S': cutoff_iso}], 'Limit': 500}
        if next_token:
            kwargs['NextToken'] = next_token

        response = dynamodb_client.execute_statement(**kwargs)
        items.extend(response.get('Items', []))
        pages_fetched += 1
        next_token = response.get('NextToken')

        if not next_token:
            break
        if pages_fetched >= MAX_CLEANUP_PAGES:
            logger.warning(
                f"Stale-ticket scan hit the {MAX_CLEANUP_PAGES}-page safety cap "
                f"({len(items)} tickets fetched) with more results still available - "
                f"some stale tickets will not be closed by this run."
            )
            break

    return items


def group_nodes_by_ticket(items):
    """Recover the ticket ID from each item's `Ticket_Number` sort key
    (stored as `"{ticket_id}_{node_id}"`) and group nodes under it.

    This strips the known `_{node_id}` suffix rather than splitting on the
    first underscore. `_SAFE_ID_PATTERN` in prequeue_validation.py allows
    underscores in ticket IDs themselves (e.g. "INC_2026_001"), so a naive
    `Ticket_Number.split('_')[0]` silently truncated any such ticket down to
    just "INC" - a peer-review finding. Since `Node_ID` is already available
    on the same item, the exact suffix to remove is known outright; no
    parsing/guessing of where the ticket ID ends is needed."""
    grouped = {}
    for item in items:
        ticket_number = item['Ticket_Number']['S']
        node_id = item['Node_ID']['S']
        suffix = f'_{node_id}'
        if not ticket_number.endswith(suffix):
            # Shouldn't happen given how Ticket_Number is constructed, but
            # if the data doesn't match the expected shape, skip it rather
            # than silently grouping under a wrong/truncated ticket ID.
            logger.warning(
                f"Ticket_Number {ticket_number!r} doesn't end with expected "
                f"suffix {suffix!r} for Node_ID {node_id!r} - skipping this item "
                f"rather than guessing its ticket ID."
            )
            continue
        ticket_id = ticket_number[:-len(suffix)]
        grouped.setdefault(ticket_id, []).append(node_id)
    return grouped


def build_dedup_id(ticket_id, nodes):
    """Stable dedup ID, same approach as the entry Lambda - if this
    schedule ever fires twice for the same stale ticket within the FIFO
    dedup window, it collapses into one message instead of two."""
    canonical = f'{ticket_id}|CLEANUP|{",".join(sorted(nodes))}'
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def publish_cleanup_message(ticket_id, nodes, correlation_id):
    """Publish a CLEANUP message directly onto the shared lifecycle queue -
    the same queue, ticket_id message group, and message shape the entry
    Lambda uses for CREATE/UPDATE/CLOSE, so the downstream processor
    doesn't need to know whether a given message came from a client or
    from this schedule, and a CLEANUP is ordered after any lifecycle
    message already in flight for that ticket."""
    queue = sqs.get_queue_by_name(QueueName=LIFECYCLE_TICKET_QUEUE)
    queue.send_message(
        MessageBody=json.dumps({
            'intent': 'CLEANUP',
            'id': ticket_id,
            'Devices': ','.join(nodes),
            'nodes': nodes,
            'correlation_id': correlation_id,
            'reason': 'STALE_TICKET_72H',
        }),
        MessageGroupId=ticket_id,
        MessageDeduplicationId=build_dedup_id(ticket_id, nodes),
    )


def lambda_handler(event, context):
    now = datetime.now(tz)
    cutoff = now - timedelta(hours=CUTOFF_TIME_HOURS)
    cutoff_iso = cutoff.isoformat()

    stale_items = find_stale_tickets(cutoff_iso)
    tickets_to_close = group_nodes_by_ticket(stale_items)

    results = {'queued': [], 'failed': []}

    for ticket_id, nodes in tickets_to_close.items():
        correlation_id = f'cleanup-{ticket_id}-{context.aws_request_id}'
        try:
            publish_cleanup_message(ticket_id, nodes, correlation_id)
            results['queued'].append(ticket_id)
        except Exception as e:
            error_summary = _safe_error_summary(e)
            logger.error("CLEANUP publish failed", extra={
                'log_ticket_id': ticket_id,
                'log_correlation_id': correlation_id,
                'log_error_summary': error_summary,
            })
            results['failed'].append({'ticket_id': ticket_id, 'error': error_summary})

    summary = (
        f"Daily cleanup: {len(results['queued'])} ticket(s) queued for closure, "
        f"{len(results['failed'])} failed, cutoff={CUTOFF_TIME_HOURS}h"
    )
    logger.info(summary, extra={'log_results': results})

    if EXCEPTION_SNS_ARN and results['failed']:
        sns_client.publish(
            TopicArn=EXCEPTION_SNS_ARN,
            Subject='Outage pipeline - cleanup partial failure',
            Message=json.dumps(results),
        )

    return {'statusCode': 200, 'body': json.dumps({'summary': summary, **results})}
