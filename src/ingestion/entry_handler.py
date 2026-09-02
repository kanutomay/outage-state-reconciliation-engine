# Name: entry_handler.py
# Component: API ingestion Lambda (behind API Gateway)
# Description: First stop for every ticket CREATE/UPDATE/CLOSE request.
#   Normalizes the incoming payload, runs pre-queue validation, and - if the
#   request passes - enqueues it to the shared lifecycle FIFO queue for
#   asynchronous processing. Returns a synchronous HTTP response either way,
#   so the caller always knows whether the request was accepted or rejected
#   before it ever reaches a queue.
#
# Security note: CLEANUP is intentionally NOT one of the intents this public
# endpoint accepts. It's a scheduled, internal-only operation performed by
# src/maintenance/ticket_cleanup_handler.py, which publishes directly onto
# the lifecycle queue via its own IAM-authorized SQS permission - it
# never goes through API Gateway, so no holder of a client API key can ever
# trigger it. See docs/known-limitations.md (L1) for the full rationale.
#
# Runtime: Python 3.13 | Memory: 256 MB | Timeout: 23s
# Trigger: API Gateway (via a lightweight router Lambda that dispatches by
#          operating company / line of business)

import os
import json
import uuid
import hashlib
import logging
from datetime import datetime, timezone, timedelta

import boto3

from prequeue_validation import run_prequeue_validation, publish_validation_metric

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.resource('sqs')
sns_client = boto3.client('sns')

# A single lifecycle FIFO queue for CREATE/UPDATE/CLOSE, grouped by
# MessageGroupId=ticket_id (see docs/architecture-overview.md - "Why one
# shared lifecycle queue instead of one per intent?"). An earlier version routed
# each intent to its own queue; FIFO only orders messages within the same
# queue *and* group, so a fast UPDATE or CLOSE sent to a different queue
# than its CREATE was never actually ordered against it - the exact
# guarantee the architecture docs claimed. Putting every lifecycle intent
# for a ticket on one queue, grouped by ticket_id, is what makes "CREATE is
# processed before its own UPDATE/CLOSE" true rather than aspirational.
LIFECYCLE_TICKET_QUEUE = os.environ.get('LIFECYCLE_TICKET_SQS', 'outage-lifecycle-ticket.fifo')
ACCEPTED_INTENTS = ('CREATE', 'UPDATE', 'CLOSE')

# A single outage ticket touching more than this many nodes doesn't reflect
# a realistic single event for this line of business - reject outright
# rather than validating (or queuing) only part of the list. Also enforced
# as a defense-in-depth cap inside prequeue_validation.py.
MAX_NODES_PER_REQUEST = 10

ERROR_SNS_TOPIC = os.environ.get('ERROR_SNS_TOPIC')  # arn:aws:sns:<region>:<account-id>:outage-error-notifications

# Fixed LOB operations run on a UTC-5 business clock; adjust for your market.
tz = timezone(timedelta(hours=-5))

HTTP_OK = 200
HTTP_ACCEPTED = 202
HTTP_BAD_REQUEST = 400
HTTP_NOT_FOUND = 404
HTTP_CONFLICT = 409
HTTP_UNPROCESSABLE = 422
HTTP_SERVER_ERROR = 500
HTTP_SERVICE_UNAVAILABLE = 503

STATUS_MAP = {
    HTTP_OK: 'SUCCESS',
    HTTP_ACCEPTED: 'ACCEPTED_NOT_PROCESSED',
    HTTP_BAD_REQUEST: 'BAD_REQUEST',
    HTTP_NOT_FOUND: 'NOT_FOUND',
    HTTP_CONFLICT: 'CONFLICT',
    HTTP_UNPROCESSABLE: 'UNPROCESSABLE_ENTITY',
    HTTP_SERVER_ERROR: 'INTERNAL_ERROR',
    HTTP_SERVICE_UNAVAILABLE: 'SERVICE_UNAVAILABLE',
}


def build_response(status_code, message, ticket_id=None, correlation_id=None, **kwargs):
    """Build the standardized response envelope every endpoint returns."""
    body = {
        'message': message,
        'status': STATUS_MAP.get(status_code, 'UNKNOWN'),
        'timestamp': datetime.now(tz).isoformat(),
    }
    if ticket_id:
        body['ticket_id'] = ticket_id
    if correlation_id:
        body['correlation_id'] = correlation_id
    body.update(kwargs)

    return {'statusCode': status_code, 'body': json.dumps(body)}


def extract_nodes(devices_field):
    """Ticket payloads carry affected network nodes as a delimited string
    (legacy format from the upstream ticketing system); normalize to a
    clean list for validation and downstream processing."""
    if not devices_field:
        return []
    return [n.strip() for n in str(devices_field).split(',') if n.strip()]


def build_dedup_id(ticket_id, intent, nodes, opco=None, category=None):
    """Stable, content-derived SQS FIFO deduplication ID.

    A previous version used a random UUID per send, which meant a genuine
    client retry of the *same* request got a *different* dedup ID every
    time - defeating FIFO's built-in dedup entirely (the very thing the
    architecture docs credited it for). Hashing the actual request content
    means an identical retry within SQS's 5-minute dedup window collapses
    into a single message, while a legitimately different request (e.g. an
    UPDATE with a changed node list) still gets its own ID.

    A later version hashed only (ticket_id, intent, nodes) - which meant two
    genuinely different requests that happened to share those three fields
    (e.g. the same ticket_id number reused across two different OpCos, or a
    CREATE retried with a corrected category) could collapse into a single
    message within the 5-minute dedup window, silently dropping the second
    request. `opco` and `category` are now part of the hash too, since
    they're both meaningful fields that distinguish otherwise-identical
    requests (see docs/api-reference.md for the request envelope).
    """
    canonical = f'{ticket_id}|{intent}|{opco or ""}|{category or ""}|{",".join(sorted(nodes))}'
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def _get_header_case_insensitive(headers, name):
    """API Gateway doesn't guarantee header-name casing is preserved (or
    consistent) across proxy integrations, and HTTP header names are
    case-insensitive by spec regardless - a literal
    `headers.get('X-Correlation-Id')` lookup silently missed a client
    sending `x-correlation-id` (the lowercase convention most HTTP clients
    and some gateways use) even though the header was present. Scan
    case-insensitively instead of trusting exact casing."""
    if not headers:
        return None
    name_lower = name.lower()
    for key, value in headers.items():
        if key.lower() == name_lower and value:
            return value
    return None


def _safe_error_summary(exc, limit=200):
    """Truncate exception text before it goes into logs or SNS. Keeps
    enough for operators to act on without echoing arbitrarily long, or
    structurally sensitive, text into a wider distribution channel."""
    text = f'{type(exc).__name__}: {exc}'
    return text if len(text) <= limit else text[:limit] + '...'


def lambda_handler(event, context):
    # Extracted before the body is parsed, so a header-supplied correlation
    # ID still comes back on a malformed-JSON error response. The
    # documented `correlation_id` body field (see docs/api-reference.md) is
    # only available once the body parses, so it's folded in as a fallback
    # right after - a header takes precedence when both are present.
    header_correlation_id = _get_header_case_insensitive(event.get('headers'), 'X-Correlation-Id')

    try:
        body = json.loads(event.get('body') or '{}')
    except (TypeError, ValueError):
        return build_response(HTTP_BAD_REQUEST, 'Malformed JSON body', correlation_id=header_correlation_id or str(uuid.uuid4()))

    correlation_id = header_correlation_id or body.get('correlation_id') or str(uuid.uuid4())

    intent = (body.get('intent') or '').upper()
    ticket_id = body.get('id')
    opco = body.get('OpCo')
    nodes = extract_nodes(body.get('Devices'))

    if intent not in ACCEPTED_INTENTS:
        # CLEANUP is deliberately excluded from this set - see the module
        # docstring. Any other unrecognized intent is just a bad request.
        return build_response(
            HTTP_BAD_REQUEST,
            f'Unsupported intent: {intent}. This endpoint accepts CREATE, UPDATE, CLOSE only; '
            f'CLEANUP is an internal-only operation and is never accepted from the public API.',
            correlation_id=correlation_id,
        )

    if not ticket_id:
        return build_response(HTTP_BAD_REQUEST, 'Missing required field: id', correlation_id=correlation_id)

    if not opco:
        # Documented as required in the request envelope (docs/api-reference.md)
        # but an earlier version never actually checked for it - it just
        # rode along unvalidated into the SQS message body.
        return build_response(HTTP_BAD_REQUEST, 'Missing required field: OpCo', ticket_id=ticket_id, correlation_id=correlation_id)

    if intent == 'CREATE' and not nodes:
        # Devices is documented as required for CREATE, but an earlier
        # version silently accepted a nodeless CREATE and let
        # run_prequeue_validation's own `if nodes else {'valid': True}`
        # guard skip the entire duplicate/conflict check for it (see
        # docs/known-limitations.md - "Major resolved issues"). A ticket with no
        # nodes can't be conflict-checked at all, so reject it outright
        # instead of accepting an unvalidated ticket.
        return build_response(
            HTTP_BAD_REQUEST,
            'Missing required field: Devices (at least one node is required for CREATE)',
            ticket_id=ticket_id,
            correlation_id=correlation_id,
        )

    if len(nodes) > MAX_NODES_PER_REQUEST:
        # Reject outright rather than silently validating/queuing a
        # truncated slice of the list - see docs/known-limitations.md.
        return build_response(
            HTTP_BAD_REQUEST,
            f'Too many nodes: {len(nodes)} supplied, {MAX_NODES_PER_REQUEST} max per request',
            ticket_id=ticket_id,
            correlation_id=correlation_id,
        )

    if intent == 'CREATE' and body.get('category') not in ('HFC Access', 'FTTH Access'):
        return build_response(
            HTTP_UNPROCESSABLE,
            'category must be one of: "HFC Access", "FTTH Access"',
            ticket_id=ticket_id,
            correlation_id=correlation_id,
        )

    # --- Pre-queue validation -------------------------------------------
    validation_check = run_prequeue_validation(intent=intent, ticket_id=ticket_id, nodes=nodes, opco=opco)

    if validation_check['should_block']:
        result = validation_check['validation_result']
        publish_validation_metric('Requests_Rejected_Prequeue', 1, {'error_type': result['error_type'], 'intent': intent})
        return build_response(
            result['error_code'],
            result['error_message'],
            ticket_id=ticket_id,
            correlation_id=correlation_id,
            error_type=result['error_type'],
            error_details=result['blocked_reason'],
        )

    # --- Enqueue for asynchronous processing -----------------------------
    try:
        queue = sqs.get_queue_by_name(QueueName=LIFECYCLE_TICKET_QUEUE)
        queue.send_message(
            MessageBody=json.dumps({**body, 'nodes': nodes, 'correlation_id': correlation_id}),
            MessageGroupId=ticket_id,                                   # FIFO ordering per ticket
            MessageDeduplicationId=build_dedup_id(ticket_id, intent, nodes, opco=opco, category=body.get('category')),
        )
    except Exception as e:
        error_summary = _safe_error_summary(e)
        logger.error("Failed to enqueue ticket", extra={
            'log_ticket_id': ticket_id,
            'log_correlation_id': correlation_id,
            'log_intent': intent,
            'log_error_type': type(e).__name__,
            'log_error_summary': error_summary,
        })
        if ERROR_SNS_TOPIC:
            sns_client.publish(
                TopicArn=ERROR_SNS_TOPIC,
                Subject=f'Outage pipeline enqueue failure - ticket {ticket_id}',
                Message=f'ticket_id={ticket_id} intent={intent} correlation_id={correlation_id} error={error_summary}',
            )
        return build_response(HTTP_SERVER_ERROR, 'Failed to queue ticket for processing', ticket_id=ticket_id, correlation_id=correlation_id)

    publish_validation_metric('Requests_Accepted_Queued', 1, {'intent': intent})

    if validation_check['shadow_mode'] and not validation_check['validation_result'].get('valid', True):
        # Shadow mode: the request would have been blocked, but validation
        # is still observing traffic before it's allowed to reject anything.
        return build_response(
            HTTP_ACCEPTED,
            'Ticket queued (shadow validation would have rejected this request)',
            ticket_id=ticket_id,
            correlation_id=correlation_id,
        )

    return build_response(HTTP_OK, 'Ticket queued successfully', ticket_id=ticket_id, correlation_id=correlation_id)
