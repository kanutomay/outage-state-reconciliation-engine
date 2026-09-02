# Name: prequeue_validation.py
# Component: Entry Lambda - Pre-Queue Validation Module
# Description: Validates ticket requests before they are enqueued to SQS FIFO
#              for downstream processing. This is the layer that prevents
#              duplicate active tickets on the same network node and rejects
#              operations against tickets that don't exist or are closed.
#
# Design principle: "Validate on Creation, Trust on Closure" - CREATE requests
# get strict duplicate/conflict checks; UPDATE and CLOSE only confirm the
# ticket exists and is in a valid state, since the upstream ticketing system
# is treated as the source of truth for a ticket's business attributes once
# it's open.
#
# IMPORTANT - what this module does NOT guarantee (see docs/known-limitations.md
# L6): the checks below are read-then-decide, not atomic. They give an
# immediate, correct-in-the-common-case answer at the API boundary, but the
# only way to make node uniqueness airtight against a genuine race between
# two concurrent requests for *different* ticket IDs is a conditional write
# at the point nodes are actually attached to a ticket - which lives in the
# downstream processor, not in this repo. Don't treat a pass here as a
# correctness guarantee on its own.

import os
import re
import boto3
import logging

logger = logging.getLogger()

# Initialize DynamoDB client
dynamodb_client = boto3.client('dynamodb')

# Configuration
TABLE_NAME = os.environ.get('OUTAGE_TICKET_TABLE', 'outage_ticket_db')
VALIDATION_MODE = os.environ.get('PREQUEUE_VALIDATION_MODE', 'enforce')
CLOSE_IDEMPOTENT = os.environ.get('CLOSE_IDEMPOTENT', 'true').lower() == 'true'

# Ticket/node identifiers are used to build PartiQL statements below. Even
# though the primary lookup path uses parameterized queries, every value
# that reaches a query is validated against this allowlist first - cheap
# insurance against malformed or hostile input, and a hard stop before
# anything gets near a string-built statement.
_SAFE_ID_PATTERN = re.compile(r'^[A-Za-z0-9_-]{1,64}$')

# Hard cap on nodes considered per validation call - also enforced upstream
# at the API entry point (src/ingestion/entry_handler.py), which rejects an
# oversized request outright before it ever reaches this module. Kept here
# too as defense-in-depth for any caller that invokes this module directly.
MAX_NODES_PER_REQUEST = 10

# Safety cap on pagination in the ticket-ID-scoped scan inside
# check_ticket_exists(). Deliberately much tighter than the cleanup
# Lambda's MAX_CLEANUP_PAGES (20): this runs synchronously in the entry
# Lambda's request path, which has a 23s total timeout (see
# src/ingestion/entry_handler.py's header), not the cleanup Lambda's 300s
# batch budget. At 200 items/page, 5 pages is 1,000 items - see
# docs/known-limitations.md (L8) for why this is a bound, not a guarantee.
MAX_TICKET_LOOKUP_PAGES = 5


class InvalidIdentifierError(ValueError):
    """Raised when a caller-supplied ticket_id/node_id doesn't match the
    expected format. Translated to an HTTP 400 by the caller - this is a
    client input problem, not an infrastructure failure."""


class ValidationBackendError(Exception):
    """Raised when DynamoDB can't be queried at all (as opposed to being
    queried successfully and returning zero results). Callers must treat
    this as fail-closed - see run_prequeue_validation()."""


def _require_safe_identifier(value, field_name):
    if not value or not _SAFE_ID_PATTERN.match(str(value)):
        raise InvalidIdentifierError(f'Invalid {field_name} format: {value!r}')


# ============================================================================
# HELPER FUNCTIONS - DynamoDB Queries
# ============================================================================

def parse_dynamodb_item(item):
    """Parse a DynamoDB item from low-level attribute-value format to a
    plain dict. Both lookup strategies below return low-level items, so both
    must go through this - a shape mismatch here (returning the raw
    {"S": "..."} wrapper instead of the plain value) previously broke status
    comparisons like `Ticket_Status == 'closed'` for the PartiQL fallback
    path.

    `opco` is intentionally lowercase, unlike every other attribute here -
    that's the table's actual attribute name, not a typo. The other fields
    use a `Pascal_Case_With_Underscores` convention; `opco` doesn't, which
    is the kind of inconsistency you get from a system with no single
    schema owner (see docs/audit-history.md - "Recovering from an
    undocumented handover")."""
    if not item:
        return None

    return {
        'Ticket_Number': item.get('Ticket_Number', {}).get('S'),
        'Node_ID': item.get('Node_ID', {}).get('S'),
        'Ticket_Status': item.get('Ticket_Status', {}).get('S'),
        'Creation_Date': item.get('Ticket_Creation_Date', {}).get('S'),
        'Closure_Date': item.get('Ticket_Closure_Date', {}).get('S'),
        'Source_System': item.get('Source_System', {}).get('S'),
        'Parent_Ticket_Number': item.get('Parent_Ticket_Number', {}).get('S'),
        'OpCo': item.get('opco', {}).get('S'),
    }


def check_ticket_exists(ticket_id, nodes=None, opco=None):
    """
    Check whether a ticket already exists in DynamoDB.

    Two lookup strategies run, and - critically - the second is not merely
    a fallback for "no nodes supplied":

    1. Partition-key query per node (fast, strongly consistent) - run first
       whenever the caller supplied a node list, since Node_ID is the
       table's partition key and Ticket_Number is stored as a sort-key
       prefix (`"{ticket_id}_{node_id}"`).
    2. A parameterized, paginated PartiQL `begins_with()` scan by ticket_id,
       run whenever step 1 didn't find a match (including when no nodes
       were supplied at all). This is intentionally more forgiving of
       formatting inconsistencies (whitespace, casing) in ticket IDs coming
       from the upstream ticketing system than a plain equality query would
       be - an early version of this lookup used a strict equality query
       and silently failed to match tickets whose IDs had incidental
       whitespace, which surfaced as false "ticket not found" errors on
       UPDATE/CLOSE.

    An earlier version treated these as mutually exclusive - if nodes were
    supplied, only step 1 ran. That let a duplicate CREATE evade detection
    whenever it named a node set that didn't overlap the ticket's *original*
    nodes (nodes drift across a ticket's life via UPDATE, so overlap with
    the submitted list was never a safe proxy for "this ticket ID exists").
    Ticket_Number is the actual identity; running the ticket_id-scoped scan
    whenever the fast path comes up empty closes that gap. The tradeoff is
    a bounded table scan on every genuinely-new ticket's CREATE path, not
    just the no-nodes case - see docs/known-limitations.md for the cost
    note and the GSI that would remove it.

    That scan is bounded and paginated (MAX_TICKET_LOOKUP_PAGES), not
    exhaustive - an earlier version made a single `Limit=200` call and
    stopped, but DynamoDB's `Limit` bounds items *evaluated per page*, not
    items *matched*: a table with more than ~200 items ahead of this
    ticket's rows in scan order could return zero matches on page one even
    though the ticket genuinely exists further in. Following `NextToken`
    up to the page cap narrows that gap considerably but doesn't close it
    outright - see docs/known-limitations.md (L8) for why "authoritative"
    was the wrong word for this lookup and what the real fix looks like.

    When `opco` is supplied, both lookup strategies additionally require a
    matching `opco` on the candidate item before treating it as a hit - see
    docs/known-limitations.md (L9) for why cross-OpCo matches would
    otherwise be a real data-isolation gap on a table shared across
    operating companies, and for why `opco` scopes data here without acting
    as an authorization credential.

    A malformed node ID in the list is rejected outright (400, via
    InvalidIdentifierError) rather than silently skipped - an earlier
    version dropped malformed entries and validated only the rest, which
    meant a request naming one well-formed node and one hostile/malformed
    one got validated as if the malformed one had simply never been sent,
    instead of being told its request was invalid.

    Args:
        ticket_id: Ticket number (e.g. "774471").
        nodes: Optional list of node IDs to check (used as a fast first pass).
        opco: Optional operating-company code to scope the match to.

    Returns:
        dict: First matching ticket, or None if genuinely not found (or
            found but under a different OpCo).

    Raises:
        InvalidIdentifierError: ticket_id or a node_id doesn't match the
            expected format.
        ValidationBackendError: DynamoDB could not be queried at all, or a
            lookup that was needed to reach a definitive answer failed.
    """
    _require_safe_identifier(ticket_id, 'ticket_id')

    if nodes:
        query_attempts = 0
        query_errors = 0

        for node_id in nodes[:MAX_NODES_PER_REQUEST]:
            _require_safe_identifier(node_id, 'node_id')
            query_attempts += 1
            try:
                kwargs = dict(
                    TableName=TABLE_NAME,
                    KeyConditionExpression='Node_ID = :node_id AND begins_with(Ticket_Number, :ticket_prefix)',
                    ExpressionAttributeValues={
                        ':node_id': {'S': node_id},
                        ':ticket_prefix': {'S': f'{ticket_id}_'},
                    },
                    Limit=5,
                    ConsistentRead=True,
                )
                if opco:
                    kwargs['FilterExpression'] = 'opco = :opco'
                    kwargs['ExpressionAttributeValues'][':opco'] = {'S': opco}
                response = dynamodb_client.query(**kwargs)
                if response.get('Items'):
                    # A definitive hit - safe to return immediately even if
                    # an earlier node in this loop failed to query, since we
                    # don't need every node checked to answer "exists".
                    return parse_dynamodb_item(response['Items'][0])
            except Exception as e:
                query_errors += 1
                logger.warning(f"Error querying node {node_id}: {str(e)}")
                continue

        if query_errors > 0:
            # At least one lookup was unresolved and we don't otherwise have
            # a definitive answer - fail closed rather than falling through
            # to the ticket_id scan on a partially-unverified result. A
            # backend outage that failed only *some* node queries is still
            # a backend outage from the caller's point of view.
            raise ValidationBackendError(
                f'{query_errors}/{query_attempts} node lookups failed for ticket {ticket_id}'
            )

        # No per-node match - fall through to the ticket_id-scoped scan
        # below rather than declaring "not found" from node overlap alone.

    # Ticket-ID-scoped lookup: catches a match regardless of node overlap
    # or which nodes (if any) were supplied, bounded by MAX_TICKET_LOOKUP_PAGES
    # rather than a single unpaginated page - see the docstring above.
    query = f'''
        SELECT "Ticket_Number", "Ticket_Status", "Ticket_Creation_Date",
               "Ticket_Closure_Date", "Source_System", "Parent_Ticket_Number", "Node_ID", "opco"
        FROM "{TABLE_NAME}"
        WHERE begins_with("Ticket_Number", ?){' AND "opco" = ?' if opco else ''}
    '''
    parameters = [{'S': f'{ticket_id}_'}]
    if opco:
        parameters.append({'S': opco})

    next_token = None
    pages_fetched = 0

    while True:
        try:
            kwargs = {
                'Statement': query,
                'Parameters': parameters,
                'ConsistentRead': True,
                'Limit': 200,  # items evaluated per page, not items matched - see docstring
            }
            if next_token:
                kwargs['NextToken'] = next_token
            response = dynamodb_client.execute_statement(**kwargs)
        except Exception as e:
            logger.error(f"PartiQL lookup failed for ticket {ticket_id}: {str(e)}")
            raise ValidationBackendError(str(e)) from e

        items = response.get('Items') or []
        if items:
            return parse_dynamodb_item(items[0])

        pages_fetched += 1
        next_token = response.get('NextToken')

        if not next_token:
            return None
        if pages_fetched >= MAX_TICKET_LOOKUP_PAGES:
            logger.warning(
                f"Ticket-ID lookup for {ticket_id!r} hit the {MAX_TICKET_LOOKUP_PAGES}-page "
                f"safety cap with more results still available - a genuinely existing ticket "
                f"beyond this point would be reported as not found. See docs/known-limitations.md (L8)."
            )
            return None


def check_nodes_have_open_tickets(nodes, opco=None):
    """
    Check whether any of the given nodes already belong to an open ticket.

    Each network node may only be attached to one active (pending/declared)
    ticket at a time - this is the core node-conflict rule that prevents
    two independently-reported tickets from double-counting the same
    outage. See the module docstring and docs/known-limitations.md (L6) for
    why this check alone doesn't fully close the race between two
    concurrent requests for different ticket IDs.

    When `opco` is supplied, only an open ticket under the same OpCo counts
    as a conflict - see docs/known-limitations.md (L9). Without this, a node
    ID reused across two operating companies sharing this table would look
    like a cross-tenant conflict even though the two OpCos' outages are
    unrelated.

    Returns:
        list[dict]: Conflicting nodes with the ticket they're already on.

    Raises:
        InvalidIdentifierError: a node_id doesn't match the expected format
            - rejected outright rather than silently skipped, so a request
            mixing one malformed node ID in with legitimate ones can't get
            validated as though the malformed one had never been sent.
        ValidationBackendError: any node lookup failed. A partial failure
            can't be treated as "no conflict on that node" - see the note
            below and docs/known-limitations.md.
    """
    conflicting_nodes = []
    query_attempts = 0
    query_errors = 0

    for node_id in nodes[:MAX_NODES_PER_REQUEST]:
        _require_safe_identifier(node_id, 'node_id')
        query_attempts += 1
        try:
            filter_expression = 'Ticket_Status IN (:pending, :declared)'
            expression_values = {
                ':node_id': {'S': node_id},
                ':pending': {'S': 'pending'},
                ':declared': {'S': 'declared'},
            }
            if opco:
                filter_expression += ' AND opco = :opco'
                expression_values[':opco'] = {'S': opco}
            response = dynamodb_client.query(
                TableName=TABLE_NAME,
                KeyConditionExpression='Node_ID = :node_id',
                FilterExpression=filter_expression,
                ExpressionAttributeValues=expression_values,
            )
            for item in response.get('Items', []):
                parsed = parse_dynamodb_item(item)
                conflicting_nodes.append({
                    'node_id': node_id,
                    'existing_ticket': parsed['Ticket_Number'],
                    'status': parsed['Ticket_Status'],
                    'creation_date': parsed['Creation_Date'],
                })
        except Exception as e:
            query_errors += 1
            logger.warning(f"Error checking node {node_id}: {str(e)}")
            continue

    if query_errors > 0:
        # Fail closed on ANY unresolved node lookup, not just "every lookup
        # failed". A security review flagged the previous "all must fail"
        # threshold: if 4 of 5 node queries succeed and find no conflict but
        # the 5th errors, that 5th node's real state is unknown - reporting
        # "no conflict" for it was a silent fail-open, not a verified result.
        raise ValidationBackendError(f'{query_errors}/{query_attempts} node-conflict lookups failed')

    return conflicting_nodes


# ============================================================================
# VALIDATION FUNCTIONS - By Intent
# ============================================================================

def validate_create_request(ticket_id, nodes, opco=None):
    """CREATE: reject if the ticket already exists, or any node is already
    attached to another open ticket. `opco` scopes both checks to the same
    operating company - see docs/known-limitations.md (L9)."""
    nodes = nodes or []  # normalize None -> [] so check_nodes_have_open_tickets() below never has to guard against it

    existing_ticket = check_ticket_exists(ticket_id, nodes, opco=opco)
    if existing_ticket:
        return {
            'valid': False,
            'error_code': 409,
            'error_type': 'TICKET_ALREADY_EXISTS',
            'error_message': f'Ticket {ticket_id} already exists in the system',
            'blocked_reason': {
                'existing_ticket': existing_ticket['Ticket_Number'],
                'existing_status': existing_ticket['Ticket_Status'],
                'created_date': existing_ticket.get('Creation_Date'),
            },
        }

    conflicting_nodes = check_nodes_have_open_tickets(nodes, opco=opco)
    if conflicting_nodes:
        return {
            'valid': False,
            'error_code': 409,
            'error_type': 'NODE_ALREADY_IN_OUTAGE',
            'error_message': f'{len(conflicting_nodes)} node(s) already have open tickets',
            'blocked_reason': {
                'conflicting_nodes': conflicting_nodes[:5],
                'total_conflicts': len(conflicting_nodes),
            },
        }

    return {'valid': True, 'error_code': None, 'error_type': None, 'error_message': None, 'blocked_reason': None}


def validate_update_request(ticket_id, nodes=None, opco=None):
    """UPDATE: the ticket must exist and must not already be closed.

    A `404` here can mean two different things that look identical to the
    caller: the ticket genuinely never existed, or it was CREATEd moments
    ago and this UPDATE's synchronous, strongly-consistent read raced ahead
    of the still-in-flight CREATE being written by the downstream processor
    (CREATE only enqueues a message; it doesn't write the ticket itself).
    This function can't tell those two cases apart, so it doesn't try to -
    see docs/known-limitations.md (L10) for why, what was considered, and
    why the caller-facing fix is "make the retry obvious", not "make the
    race disappear"."""

    existing_ticket = check_ticket_exists(ticket_id, nodes, opco=opco)
    if not existing_ticket:
        return {
            'valid': False,
            'error_code': 404,
            'error_type': 'TICKET_NOT_FOUND',
            'error_message': f'Ticket {ticket_id} not found in the system',
            'blocked_reason': {
                'searched_ticket': ticket_id,
                'note': (
                    'If this ticket was created moments ago, the CREATE may still be '
                    'in flight - the create only queues the ticket for processing and '
                    'does not write it synchronously. Retry shortly before assuming the '
                    'ticket ID is wrong. See docs/known-limitations.md (L10).'
                ),
            },
        }

    if existing_ticket.get('Ticket_Status') == 'closed':
        return {
            'valid': False,
            'error_code': 422,
            'error_type': 'INVALID_TICKET_STATE',
            'error_message': f'Cannot update ticket {ticket_id} - already closed',
            'blocked_reason': {
                'current_status': 'closed',
                'closed_date': existing_ticket.get('Closure_Date'),
            },
        }

    return {'valid': True, 'error_code': None, 'error_type': None, 'error_message': None, 'blocked_reason': None}


def validate_close_request(ticket_id, nodes=None, opco=None):
    """CLOSE: the ticket must exist. Closing an already-closed ticket is
    treated as idempotent success by default (CLOSE_IDEMPOTENT=true), since
    upstream systems retry CLOSE calls and a hard rejection just pushes the
    same problem back to the caller.

    A `404` here carries the same same-instant-as-CREATE ambiguity documented
    on validate_update_request() above - see docs/known-limitations.md (L10)."""

    existing_ticket = check_ticket_exists(ticket_id, nodes, opco=opco)
    if not existing_ticket:
        return {
            'valid': False,
            'error_code': 404,
            'error_type': 'TICKET_NOT_FOUND',
            'error_message': f'Ticket {ticket_id} not found in the system',
            'blocked_reason': {
                'searched_ticket': ticket_id,
                'note': (
                    'If this ticket was created moments ago, the CREATE may still be '
                    'in flight - the create only queues the ticket for processing and '
                    'does not write it synchronously. Retry shortly before assuming the '
                    'ticket ID is wrong. See docs/known-limitations.md (L10).'
                ),
            },
        }

    if existing_ticket.get('Ticket_Status') == 'closed':
        if CLOSE_IDEMPOTENT:
            return {
                'valid': True,
                'error_code': None,
                'error_type': None,
                'error_message': None,
                'blocked_reason': None,
                'idempotent_close': True,
            }
        return {
            'valid': False,
            'error_code': 409,
            'error_type': 'TICKET_ALREADY_CLOSED',
            'error_message': f'Ticket {ticket_id} is already closed',
            'blocked_reason': {
                'current_status': 'closed',
                'closed_date': existing_ticket.get('Closure_Date'),
            },
        }

    return {'valid': True, 'error_code': None, 'error_type': None, 'error_message': None, 'blocked_reason': None}


# ============================================================================
# MAIN VALIDATION ORCHESTRATOR
# ============================================================================

def run_prequeue_validation(intent, ticket_id, nodes=None, validation_mode=None, opco=None):
    """
    Run pre-queue validation with shadow-mode support, so a new or changed
    validation rule can be rolled out observing production traffic before
    it's allowed to actually reject requests.

    Args:
        intent: "CREATE" | "UPDATE" | "CLOSE" | "CLEANUP"
        ticket_id: Ticket number.
        nodes: List of node IDs (required for CREATE).
        validation_mode: "enforce" | "shadow" (defaults to env var).
        opco: Operating-company/tenant code, required by the entry Lambda for
            every intent and threaded through to scope every duplicate/
            conflict/existence check to the same OpCo - see
            docs/known-limitations.md (L9).

    Returns:
        dict: {'should_block': bool, 'validation_result': dict, 'shadow_mode': bool}
    """
    if validation_mode is None:
        validation_mode = VALIDATION_MODE

    # CLEANUP is never accepted on the public API at all (the entry Lambda
    # rejects it before this module is even called - see
    # src/ingestion/entry_handler.py and docs/known-limitations.md L1). This
    # branch is retained only as defense-in-depth for a hypothetical future
    # internal caller that invokes this module directly outside the HTTP
    # entry point.
    if intent == 'CLEANUP':
        return {'should_block': False, 'validation_result': {'valid': True}, 'shadow_mode': False}

    try:
        if intent == 'CREATE':
            # `nodes` is documented as required for CREATE. An earlier
            # version skipped validate_create_request() entirely when it
            # was empty (`if nodes else {'valid': True}`), which meant a
            # CREATE with no nodes bypassed both the duplicate-ticket check
            # and the node-conflict check outright - the exact validation
            # this branch exists to perform. src/ingestion/entry_handler.py
            # now rejects a nodeless CREATE with 400 before this module is
            # even called, but this module doesn't rely on that alone: it
            # always runs the real check now, both as defense-in-depth and
            # because validate_create_request()/check_ticket_exists() both
            # already handle an empty node list correctly (falling back to
            # the ticket-ID-scoped lookup rather than crashing).
            validation_result = validate_create_request(ticket_id, nodes, opco=opco)
        elif intent == 'UPDATE':
            validation_result = validate_update_request(ticket_id, nodes, opco=opco)
        elif intent == 'CLOSE':
            validation_result = validate_close_request(ticket_id, nodes, opco=opco)
        else:
            return {'should_block': False, 'validation_result': {'valid': True}, 'shadow_mode': False}

    except InvalidIdentifierError as e:
        # Client sent something malformed - a normal 400, not an outage.
        return {
            'should_block': True,
            'validation_result': {
                'valid': False,
                'error_code': 400,
                'error_type': 'INVALID_IDENTIFIER',
                'error_message': str(e),
                'blocked_reason': None,
            },
            'shadow_mode': False,
        }

    except Exception as e:
        # Fail CLOSED: if we can't reliably answer "does this ticket/node
        # conflict exist?", the safe default for outage state is to reject
        # and ask the caller to retry - not to silently let a request
        # through that we had no way to actually check. This intentionally
        # overrides shadow mode: an infrastructure failure isn't a business
        # rule being rolled out, so shadow mode's "log but allow" semantics
        # don't apply here.
        logger.error("Validation backend unavailable - failing closed", extra={
            'intent': intent, 'ticket_id': ticket_id, 'error_type': type(e).__name__, 'error': str(e)[:200],
        })
        return {
            'should_block': True,
            'validation_result': {
                'valid': False,
                'error_code': 503,
                'error_type': 'VALIDATION_SERVICE_UNAVAILABLE',
                'error_message': 'Unable to validate ticket state right now. Retry shortly.',
                'blocked_reason': None,
            },
            'shadow_mode': False,
            'infra_failure': True,
        }

    shadow_mode = (validation_mode == 'shadow')
    should_block = (not validation_result['valid']) and (not shadow_mode)

    if not validation_result['valid']:
        log_data = {
            'intent': intent,
            'ticket_id': ticket_id,
            'error_type': validation_result.get('error_type'),
            'shadow_mode': shadow_mode,
            'would_block': not shadow_mode,
        }
        if shadow_mode:
            logger.warning(f"[SHADOW MODE] Would have blocked request: {validation_result.get('error_type')}", extra=log_data)
        else:
            logger.error(f"[ENFORCE MODE] Blocking request: {validation_result.get('error_type')}", extra=log_data)

    return {'should_block': should_block, 'validation_result': validation_result, 'shadow_mode': shadow_mode}


# ============================================================================
# CLOUDWATCH METRICS
# ============================================================================

def publish_validation_metric(metric_name, value, dimensions):
    """Publish a custom CloudWatch metric for validation outcomes."""
    try:
        cloudwatch = boto3.client('cloudwatch')
        cloudwatch.put_metric_data(
            Namespace='OutageEngine/Validation',
            MetricData=[{
                'MetricName': metric_name,
                'Value': value,
                'Unit': 'Count',
                'Dimensions': [{'Name': k, 'Value': v} for k, v in dimensions.items()],
            }],
        )
    except Exception as e:
        logger.warning(f"Failed to publish metric: {str(e)}")
