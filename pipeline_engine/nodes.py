"""Built-in node types for Pipeline workflows."""

import re
import json
import time
import logging
import urllib.request
import urllib.error
from typing import Optional, Callable

from pipeline_engine.models import NodeResult
from pipeline_engine.expressions import resolve_dict, resolve_template

logger = logging.getLogger("pipeline.nodes")

# ── Node Registry ──────────────────────────────────────────────────────────

NODE_REGISTRY: dict[str, type] = {}


def register_node(cls):
    """Decorator to register a node type."""
    NODE_REGISTRY[cls.node_type] = cls
    return cls


class BaseNode:
    """Abstract base for all node types."""
    node_type: str = ""

    def execute(self, config: dict, input_data: dict, context: dict) -> NodeResult:
        raise NotImplementedError


# ── Built-in Nodes ─────────────────────────────────────────────────────────

@register_node
class HttpRequestNode(BaseNode):
    node_type = "http_request"

    def execute(self, config: dict, input_data: dict, context: dict) -> NodeResult:
        url = resolve_template(config.get("url", ""), context)
        method = config.get("method", "GET").upper()
        headers = resolve_dict(config.get("headers", {}), context)
        body = config.get("body")
        timeout = config.get("timeout_ms", 30000) / 1000

        if body:
            body = resolve_dict(body, context)

        try:
            data = json.dumps(body).encode() if body else None
            req = urllib.request.Request(url, data=data, method=method)
            for k, v in headers.items():
                req.add_header(k, str(v))
            if data and "Content-Type" not in headers:
                req.add_header("Content-Type", "application/json")

            with urllib.request.urlopen(req, timeout=timeout) as resp:
                resp_body = resp.read().decode("utf-8")
                try:
                    resp_json = json.loads(resp_body)
                except json.JSONDecodeError:
                    resp_json = resp_body

                return NodeResult(
                    output={"status": resp.status, "body": resp_json, "headers": dict(resp.headers)},
                    status="completed",
                )
        except urllib.error.HTTPError as e:
            return NodeResult(
                output={"status": e.code, "error": str(e)},
                status="failed",
                error=f"HTTP {e.code}: {e.reason}",
            )
        except Exception as e:
            return NodeResult(status="failed", error=str(e))


@register_node
class TransformNode(BaseNode):
    node_type = "transform"

    def execute(self, config: dict, input_data: dict, context: dict) -> NodeResult:
        expression = config.get("expression", "")
        if expression:
            result = resolve_dict(expression, context)
            return NodeResult(output={"result": result}, status="completed")

        mapping = config.get("mapping", {})
        output = {}
        for key, expr in mapping.items():
            if isinstance(expr, str):
                output[key] = resolve_template(expr, context)
            else:
                output[key] = resolve_dict(expr, context)
        return NodeResult(output=output, status="completed")


@register_node
class ConditionNode(BaseNode):
    node_type = "condition"

    def execute(self, config: dict, input_data: dict, context: dict) -> NodeResult:
        from pipeline_engine.expressions import evaluate_condition
        expression = config.get("expression", "true")
        result = evaluate_condition(expression, context)
        return NodeResult(
            output={"result": result, "branch": "true" if result else "false"},
            status="completed",
        )


@register_node
class DelayNode(BaseNode):
    node_type = "delay"

    def execute(self, config: dict, input_data: dict, context: dict) -> NodeResult:
        delay_ms = config.get("delay_ms", 0)
        wait_until = config.get("wait_until")

        if wait_until:
            return NodeResult(status="waiting", wait_until=resolve_template(wait_until, context))
        elif delay_ms > 0:
            # Calculate future time
            future = time.time() + (delay_ms / 1000)
            wait_time = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(future))
            return NodeResult(status="waiting", wait_until=wait_time)

        return NodeResult(output={"delayed": False}, status="completed")


@register_node
class FanInNode(BaseNode):
    node_type = "fan_in"

    def execute(self, config: dict, input_data: dict, context: dict) -> NodeResult:
        wait_for = config.get("wait_for", [])
        merge_strategy = config.get("merge_strategy", "object")

        merged = {}
        for step_id in wait_for:
            step_output = context.get("nodes", {}).get(step_id, {}).get("output", {})
            if merge_strategy == "object":
                merged[step_id] = step_output
            elif merge_strategy == "array":
                merged.setdefault("items", []).append(step_output)
            elif merge_strategy == "flat":
                merged.update(step_output)

        return NodeResult(output=merged, status="completed")


@register_node
class SetVariableNode(BaseNode):
    node_type = "set_variable"

    def execute(self, config: dict, input_data: dict, context: dict) -> NodeResult:
        variables = resolve_dict(config.get("variables", {}), context)
        return NodeResult(output=variables, status="completed")


@register_node
class NoopNode(BaseNode):
    node_type = "noop"

    def execute(self, config: dict, input_data: dict, context: dict) -> NodeResult:
        return NodeResult(output=input_data or {}, status="completed")


@register_node
class AgentNode(BaseNode):
    node_type = "agent"

    def execute(self, config: dict, input_data: dict, context: dict) -> NodeResult:
        prompt = resolve_template(config.get("prompt", ""), context)
        llm_fn = context.get("_llm_fn")

        if not llm_fn:
            return NodeResult(status="failed", error="No llm_fn provided for agent node")

        try:
            response = llm_fn(prompt)
            return NodeResult(output={"response": response, "prompt": prompt}, status="completed")
        except Exception as e:
            return NodeResult(status="failed", error=f"Agent call failed: {e}")


@register_node
class SwitchNode(BaseNode):
    node_type = "switch"

    def execute(self, config: dict, input_data: dict, context: dict) -> NodeResult:
        from pipeline_engine.expressions import evaluate_condition
        cases = config.get("cases", [])
        default_branch = config.get("default", "default")

        for case in cases:
            condition = case.get("condition", "")
            if evaluate_condition(condition, context):
                return NodeResult(
                    output={"matched_case": case.get("label", ""), "branch": case.get("target", "")},
                    status="completed",
                )

        return NodeResult(
            output={"matched_case": "default", "branch": default_branch},
            status="completed",
        )


@register_node
class ScriptNode(BaseNode):
    """Sandboxed expression evaluation — NOT arbitrary code execution."""
    node_type = "script"

    def execute(self, config: dict, input_data: dict, context: dict) -> NodeResult:
        expression = config.get("expression", "")
        result = resolve_dict(expression, context)
        return NodeResult(output={"result": result}, status="completed")


def get_node_executor(node_type: str) -> Optional[BaseNode]:
    """Get a node executor by type."""
    cls = NODE_REGISTRY.get(node_type)
    return cls() if cls else None
