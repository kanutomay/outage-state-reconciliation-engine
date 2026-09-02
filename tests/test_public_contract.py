"""Sanitized executable tests for the public portfolio reconstruction.

These tests deliberately use small in-memory AWS doubles. They validate the
included code's public contract without AWS credentials, third-party test
packages, or any production identifiers.
"""

import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import Mock


ROOT = Path(__file__).resolve().parents[1]


class FakeQueue:
    def __init__(self):
        self.messages = []

    def send_message(self, **kwargs):
        self.messages.append(kwargs)
        return {"MessageId": "example-message-id"}


class FakeSqs:
    def __init__(self):
        self.queue = FakeQueue()

    def get_queue_by_name(self, **_kwargs):
        return self.queue


fake_sqs = FakeSqs()
fake_boto3 = types.ModuleType("boto3")
fake_boto3.resource = Mock(return_value=fake_sqs)
fake_boto3.client = Mock(return_value=Mock())
sys.modules["boto3"] = fake_boto3

validation_double = types.ModuleType("prequeue_validation")
validation_double.run_prequeue_validation = Mock()
validation_double.publish_validation_metric = Mock()
sys.modules["prequeue_validation"] = validation_double


def load_module(name, relative_path):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


entry = load_module("portfolio_entry_handler", "src/ingestion/entry_handler.py")
cleanup = load_module("portfolio_cleanup_handler", "src/maintenance/ticket_cleanup_handler.py")


def allow_validation():
    return {
        "should_block": False,
        "shadow_mode": False,
        "validation_result": {"valid": True},
    }


class EntryContractTests(unittest.TestCase):
    def setUp(self):
        fake_sqs.queue.messages.clear()
        validation_double.run_prequeue_validation.reset_mock()
        validation_double.run_prequeue_validation.return_value = allow_validation()
        validation_double.publish_validation_metric.reset_mock()

    @staticmethod
    def invoke(payload, headers=None):
        return entry.lambda_handler(
            {"body": json.dumps(payload), "headers": headers or {}},
            Mock(aws_request_id="request-1"),
        )

    def test_dedup_id_is_order_independent_for_nodes(self):
        first = entry.build_dedup_id("T-1", "CREATE", ["NODE-B", "NODE-A"], "ACME", "HFC Access")
        second = entry.build_dedup_id("T-1", "CREATE", ["NODE-A", "NODE-B"], "ACME", "HFC Access")
        self.assertEqual(first, second)

    def test_dedup_id_distinguishes_tenant_and_category(self):
        baseline = entry.build_dedup_id("T-1", "CREATE", ["NODE-A"], "ACME", "HFC Access")
        self.assertNotEqual(baseline, entry.build_dedup_id("T-1", "CREATE", ["NODE-A"], "OTHER", "HFC Access"))
        self.assertNotEqual(baseline, entry.build_dedup_id("T-1", "CREATE", ["NODE-A"], "ACME", "FTTH Access"))

    def test_cleanup_is_rejected_at_public_boundary(self):
        response = self.invoke({"intent": "CLEANUP", "id": "T-1", "OpCo": "ACME"})
        self.assertEqual(400, response["statusCode"])
        self.assertIn("internal-only", json.loads(response["body"])["message"])

    def test_create_requires_nodes(self):
        response = self.invoke({"intent": "CREATE", "id": "T-1", "OpCo": "ACME", "category": "HFC Access"})
        self.assertEqual(400, response["statusCode"])
        validation_double.run_prequeue_validation.assert_not_called()

    def test_opco_is_required(self):
        response = self.invoke({"intent": "CLOSE", "id": "T-1"})
        self.assertEqual(400, response["statusCode"])

    def test_node_cap_is_enforced_before_validation(self):
        devices = ",".join(f"NODE-{index}" for index in range(11))
        response = self.invoke({"intent": "CREATE", "id": "T-1", "OpCo": "ACME", "category": "HFC Access", "Devices": devices})
        self.assertEqual(400, response["statusCode"])
        validation_double.run_prequeue_validation.assert_not_called()

    def test_valid_create_is_queued_in_ticket_group(self):
        response = self.invoke({"intent": "CREATE", "id": "T-1", "OpCo": "ACME", "category": "HFC Access", "Devices": "NODE-B,NODE-A"})
        self.assertEqual(200, response["statusCode"])
        sent = fake_sqs.queue.messages[-1]
        self.assertEqual("T-1", sent["MessageGroupId"])
        self.assertEqual(64, len(sent["MessageDeduplicationId"]))
        self.assertEqual(["NODE-B", "NODE-A"], json.loads(sent["MessageBody"])["nodes"])

    def test_validation_rejection_is_returned_synchronously(self):
        validation_double.run_prequeue_validation.return_value = {
            "should_block": True,
            "shadow_mode": False,
            "validation_result": {
                "valid": False,
                "error_code": 409,
                "error_message": "Node already has an open outage",
                "error_type": "NODE_ALREADY_IN_OUTAGE",
                "blocked_reason": {"node_id": "NODE-A"},
            },
        }
        response = self.invoke({"intent": "CREATE", "id": "T-2", "OpCo": "ACME", "category": "HFC Access", "Devices": "NODE-A"})
        self.assertEqual(409, response["statusCode"])
        self.assertEqual("NODE_ALREADY_IN_OUTAGE", json.loads(response["body"])["error_type"])
        self.assertEqual([], fake_sqs.queue.messages)

    def test_lowercase_correlation_header_is_preserved(self):
        response = self.invoke(
            {"intent": "CLOSE", "id": "T-1", "OpCo": "ACME"},
            {"x-correlation-id": "caller-trace-123"},
        )
        self.assertEqual("caller-trace-123", json.loads(response["body"])["correlation_id"])
        self.assertEqual("caller-trace-123", json.loads(fake_sqs.queue.messages[-1]["MessageBody"])["correlation_id"])


class CleanupContractTests(unittest.TestCase):
    def test_ticket_ids_with_underscores_are_not_truncated(self):
        items = [
            {
                "Ticket_Number": {"S": "INC_2026_001_NODE-A"},
                "Node_ID": {"S": "NODE-A"},
            }
        ]
        self.assertEqual({"INC_2026_001": ["NODE-A"]}, cleanup.group_nodes_by_ticket(items))

    def test_malformed_ticket_node_shape_is_skipped(self):
        items = [
            {
                "Ticket_Number": {"S": "T-1_NODE-A"},
                "Node_ID": {"S": "NODE-B"},
            }
        ]
        self.assertEqual({}, cleanup.group_nodes_by_ticket(items))


if __name__ == "__main__":
    unittest.main()
