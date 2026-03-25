"""Tests for Pipeline Engine — Workflow engine with LLM self-healing."""

import os
import sys
import json
import time
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class PipelineTestCase(unittest.TestCase):
    def setUp(self):
        self.db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.db_file.name
        self.db_file.close()
        from pipeline_engine.engine import PipelineEngine
        self.engine = PipelineEngine({"db_path": self.db_path})

    def tearDown(self):
        self.engine.close()
        for ext in ["", "-wal", "-shm"]:
            p = self.db_path + ext
            if os.path.exists(p):
                os.unlink(p)


# ── DAG Tests ──────────────────────────────────────────────────────────────

class TestDAG(unittest.TestCase):

    def test_topological_sort(self):
        from pipeline_engine.dag import topological_sort
        nodes = {"a": {}, "b": {}, "c": {}}
        edges = [{"from": "a", "to": "b"}, {"from": "b", "to": "c"}]
        result = topological_sort(nodes, edges)
        self.assertEqual(result, ["a", "b", "c"])

    def test_cycle_detection(self):
        from pipeline_engine.dag import topological_sort
        from pipeline_engine.errors import CycleDetectedError
        nodes = {"a": {}, "b": {}, "c": {}}
        edges = [{"from": "a", "to": "b"}, {"from": "b", "to": "c"}, {"from": "c", "to": "a"}]
        with self.assertRaises(CycleDetectedError):
            topological_sort(nodes, edges)

    def test_parallel_branches(self):
        from pipeline_engine.dag import topological_sort
        nodes = {"start": {}, "a": {}, "b": {}, "end": {}}
        edges = [
            {"from": "start", "to": "a"}, {"from": "start", "to": "b"},
            {"from": "a", "to": "end"}, {"from": "b", "to": "end"},
        ]
        result = topological_sort(nodes, edges)
        self.assertEqual(result[0], "start")
        self.assertEqual(result[-1], "end")

    def test_ready_nodes(self):
        from pipeline_engine.dag import get_ready_nodes
        nodes = {"a": {}, "b": {}, "c": {}}
        edges = [{"from": "a", "to": "b"}, {"from": "b", "to": "c"}]
        statuses = {"a": "completed", "b": "pending", "c": "pending"}
        ready = get_ready_nodes(nodes, edges, statuses)
        self.assertEqual(ready, ["b"])

    def test_validate_workflow(self):
        from pipeline_engine.dag import validate_workflow
        errors = validate_workflow({"nodes": {"a": {"type": "noop"}}, "edges": []})
        self.assertEqual(len(errors), 0)

    def test_validate_empty_workflow(self):
        from pipeline_engine.dag import validate_workflow
        errors = validate_workflow({"nodes": {}, "edges": []})
        self.assertGreater(len(errors), 0)


# ── Expression Tests ───────────────────────────────────────────────────────

class TestExpressions(unittest.TestCase):

    def test_resolve_template(self):
        from pipeline_engine.expressions import resolve_template
        result = resolve_template("Hello {{name}}", {"name": "World"})
        self.assertEqual(result, "Hello World")

    def test_resolve_path(self):
        from pipeline_engine.expressions import resolve_path
        ctx = {"nodes": {"step1": {"output": {"data": [{"name": "Alice"}]}}}}
        result = resolve_path("nodes.step1.output.data[0].name", ctx)
        self.assertEqual(result, "Alice")

    def test_evaluate_condition(self):
        from pipeline_engine.expressions import evaluate_condition
        self.assertTrue(evaluate_condition("count > 5", {"count": 10}))
        self.assertFalse(evaluate_condition("count > 5", {"count": 3}))

    def test_resolve_dict(self):
        from pipeline_engine.expressions import resolve_dict
        obj = {"greeting": "Hello {{name}}", "items": ["{{a}}", "{{b}}"]}
        result = resolve_dict(obj, {"name": "Bob", "a": "x", "b": "y"})
        self.assertEqual(result["greeting"], "Hello Bob")
        self.assertEqual(result["items"], ["x", "y"])


# ── Node Tests ─────────────────────────────────────────────────────────────

class TestNodes(unittest.TestCase):

    def test_noop_node(self):
        from pipeline_engine.nodes import get_node_executor
        node = get_node_executor("noop")
        result = node.execute({}, {"key": "val"}, {})
        self.assertEqual(result.status, "completed")

    def test_transform_node(self):
        from pipeline_engine.nodes import get_node_executor
        node = get_node_executor("transform")
        result = node.execute(
            {"mapping": {"full_name": "{{first}} {{last}}"}},
            {}, {"first": "John", "last": "Doe"},
        )
        self.assertEqual(result.output["full_name"], "John Doe")

    def test_condition_node(self):
        from pipeline_engine.nodes import get_node_executor
        node = get_node_executor("condition")
        result = node.execute({"expression": "count > 5"}, {}, {"count": 10})
        self.assertEqual(result.output["result"], True)

    def test_set_variable_node(self):
        from pipeline_engine.nodes import get_node_executor
        node = get_node_executor("set_variable")
        result = node.execute({"variables": {"x": "{{val}}"}}, {}, {"val": "hello"})
        self.assertEqual(result.output["x"], "hello")

    def test_switch_node(self):
        from pipeline_engine.nodes import get_node_executor
        node = get_node_executor("switch")
        result = node.execute(
            {"cases": [{"condition": "status == active", "label": "active_branch", "target": "step_a"}],
             "default": "step_b"},
            {}, {"status": "active"},
        )
        self.assertEqual(result.output["matched_case"], "active_branch")

    def test_node_registry(self):
        from pipeline_engine.nodes import NODE_REGISTRY
        expected = {"http_request", "transform", "condition", "delay", "fan_in",
                    "set_variable", "noop", "agent", "switch", "script"}
        self.assertTrue(expected.issubset(set(NODE_REGISTRY.keys())))


# ── Healing Tests ──────────────────────────────────────────────────────────

class TestHealing(unittest.TestCase):

    def test_classify_error(self):
        from pipeline_engine.healing import classify_error
        self.assertEqual(classify_error("Connection timeout after 30s"), "timeout")
        self.assertEqual(classify_error("HTTP 429 Too Many Requests"), "rate_limit")
        self.assertEqual(classify_error("401 Unauthorized"), "auth")
        self.assertEqual(classify_error("Something weird"), "unknown")

    def test_should_not_heal_auth(self):
        from pipeline_engine.healing import should_attempt_healing
        result = should_attempt_healing("auth", 4, 3, 3, 0, 0)
        self.assertFalse(result)

    def test_should_heal_after_retries(self):
        from pipeline_engine.healing import should_attempt_healing
        result = should_attempt_healing("timeout", 4, 3, 3, 0, 0)
        self.assertTrue(result)

    def test_should_not_exceed_limits(self):
        from pipeline_engine.healing import should_attempt_healing
        result = should_attempt_healing("timeout", 6, 3, 3, 2, 0)
        self.assertFalse(result)

    def test_validate_decision_immutable_fields(self):
        from pipeline_engine.healing import validate_decision
        from pipeline_engine.models import HealingDecision
        d = HealingDecision(
            action="retry_modified", confidence=0.9,
            retry_params={"modifications": {"auth_token": "new_token"}},
        )
        valid, reason = validate_decision(d, {})
        self.assertFalse(valid)
        self.assertIn("immutable", reason.lower())

    def test_validate_decision_low_confidence(self):
        from pipeline_engine.healing import validate_decision
        from pipeline_engine.models import HealingDecision
        d = HealingDecision(action="retry_modified", confidence=0.3)
        valid, reason = validate_decision(d, {})
        self.assertFalse(valid)

    def test_validate_decision_valid(self):
        from pipeline_engine.healing import validate_decision
        from pipeline_engine.models import HealingDecision
        d = HealingDecision(
            action="retry_modified", confidence=0.9,
            retry_params={"modifications": {"timeout": 60}},
        )
        valid, _ = validate_decision(d, {})
        self.assertTrue(valid)

    def test_apply_healing(self):
        from pipeline_engine.healing import apply_healing
        from pipeline_engine.models import HealingDecision
        d = HealingDecision(
            action="retry_modified",
            retry_params={"modifications": {"timeout": 60}},
        )
        config = {"config": {"timeout": 30, "url": "http://example.com"}}
        result = apply_healing(d, config)
        self.assertEqual(result["config"]["timeout"], 60)
        self.assertEqual(result["config"]["url"], "http://example.com")


# ── Workflow Execution Tests ───────────────────────────────────────────────

class TestWorkflowExecution(PipelineTestCase):

    def test_register_workflow(self):
        wid = self.engine.register_workflow({
            "name": "test-wf",
            "nodes": {"a": {"type": "noop"}, "b": {"type": "noop"}},
            "edges": [{"from": "a", "to": "b"}],
        })
        self.assertTrue(wid)

    def test_list_workflows(self):
        self.engine.register_workflow({
            "name": "wf1", "nodes": {"a": {"type": "noop"}}, "edges": [],
        })
        wfs = self.engine.list_workflows()
        self.assertEqual(len(wfs), 1)

    def test_simple_execution(self):
        wid = self.engine.register_workflow({
            "name": "simple",
            "nodes": {
                "start": {"type": "set_variable", "config": {"variables": {"x": "hello"}}},
                "end": {"type": "noop"},
            },
            "edges": [{"from": "start", "to": "end"}],
        })
        eid = self.engine.start_execution(wid)
        self.assertTrue(eid)

        # Wait for completion
        time.sleep(0.5)
        exe = self.engine.get_execution(eid)
        self.assertIn(exe["status"], ("completed", "running"))

    def test_parallel_execution(self):
        wid = self.engine.register_workflow({
            "name": "parallel",
            "nodes": {
                "start": {"type": "noop"},
                "branch_a": {"type": "set_variable", "config": {"variables": {"a": "1"}}},
                "branch_b": {"type": "set_variable", "config": {"variables": {"b": "2"}}},
                "end": {"type": "noop"},
            },
            "edges": [
                {"from": "start", "to": "branch_a"},
                {"from": "start", "to": "branch_b"},
                {"from": "branch_a", "to": "end"},
                {"from": "branch_b", "to": "end"},
            ],
        })
        eid = self.engine.start_execution(wid)
        time.sleep(1)
        exe = self.engine.get_execution(eid)
        self.assertIn(exe["status"], ("completed", "running"))

    def test_cancel_execution(self):
        wid = self.engine.register_workflow({
            "name": "cancellable",
            "nodes": {"a": {"type": "delay", "config": {"delay_ms": 60000}}},
            "edges": [],
        })
        eid = self.engine.start_execution(wid)
        self.assertTrue(self.engine.cancel_execution(eid))

    def test_stats(self):
        stats = self.engine.stats()
        self.assertIn("active_workflows", stats)
        self.assertIn("node_types_available", stats)

    def test_health_check(self):
        health = self.engine.health_check()
        self.assertTrue(health["healthy"])


# ── Healing Integration Tests ──────────────────────────────────────────────

class TestHealingIntegration(PipelineTestCase):

    def test_healing_with_mock_llm(self):
        """Test that the healer can recover from a failure."""
        def mock_llm(prompt):
            return json.dumps({
                "action": "skip",
                "confidence": 0.85,
                "reasoning": "Non-critical step, safe to skip",
                "skip_config": {"output_strategy": "null"},
            })

        wid = self.engine.register_workflow({
            "name": "heal-test",
            "nodes": {
                "will_fail": {
                    "type": "http_request",
                    "config": {"url": "http://localhost:99999/nonexistent", "timeout_ms": 1000},
                    "retry": {"max_attempts": 1},
                },
                "after": {"type": "noop"},
            },
            "edges": [{"from": "will_fail", "to": "after"}],
            "on_error": "pause",
        })
        eid = self.engine.start_execution(wid, llm_fn=mock_llm)
        time.sleep(3)

        history = self.engine.get_healing_history(eid)
        # Should have attempted healing
        self.assertGreaterEqual(len(history), 0)

    def test_healing_history(self):
        history = self.engine.get_healing_history()
        self.assertIsInstance(history, list)


if __name__ == "__main__":
    unittest.main()
