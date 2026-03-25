"""LLM-powered self-healing for workflow failures.

When a step fails, instead of just retrying:
1. Classifies the error (network, auth, validation, business logic, timeout)
2. Asks the LLM for a recovery decision
3. Validates the LLM's suggestion against guardrails
4. Applies the decision (retry_modified, skip, reroute, escalate, abort)
5. Logs everything for auditability

Budget-conscious: only invokes LLM after normal retries are exhausted.
"""

import re
import json
import time
import logging
from typing import Optional, Callable

from pipeline_engine.models import HealingDecision, NodeResult
from pipeline_engine.errors import HealingError, HealingLimitExceededError

logger = logging.getLogger("pipeline.healing")

# ── Error Classification ───────────────────────────────────────────────────

ERROR_PATTERNS = {
    "timeout": [r"timeout", r"timed out", r"ETIMEDOUT", r"deadline exceeded"],
    "network": [r"connection refused", r"ECONNREFUSED", r"DNS", r"unreachable", r"fetch failed"],
    "auth": [r"401", r"403", r"unauthorized", r"forbidden", r"auth", r"token expired"],
    "rate_limit": [r"429", r"too many requests", r"rate limit", r"throttle"],
    "not_found": [r"404", r"not found"],
    "server_error": [r"500", r"502", r"503", r"504", r"internal server error", r"bad gateway"],
    "validation": [r"validation", r"invalid", r"malformed", r"schema", r"type error"],
}

# Errors that should NOT trigger LLM healing
NON_HEALABLE = {"auth"}  # Auth failures need human intervention

# Max healing attempts per step per execution
MAX_HEALING_ATTEMPTS = 2
MAX_LLM_CALLS_PER_EXECUTION = 5

# Confidence thresholds
AUTO_EXECUTE_THRESHOLD = 0.80
ESCALATE_THRESHOLD = 0.50

# Fields the LLM is NOT allowed to modify
IMMUTABLE_FIELDS = frozenset({
    "auth", "token", "secret", "password", "key", "credential",
    "api_key", "api_secret", "private_key", "bearer",
})

HEALING_PROMPT = """You are Pipeline's Healing Assistant. Analyze this workflow step failure and suggest a safe recovery.

## FAILURE CONTEXT
Step: {node_id} (type: {node_type})
Error: {error_message}
Error category: {error_category}
Attempt: {attempt} of {max_attempts}
Previous healing attempts: {previous_attempts}

## STEP CONFIGURATION
{step_config}

## AVAILABLE ACTIONS
1. retry_modified - Adjust parameters and retry (e.g., increase timeout, change headers)
2. skip - Skip this step if non-critical (provide mock output)
3. reroute - Switch to alternate path (targets: {reroute_targets})
4. escalate - Pause for human review
5. abort - Stop workflow gracefully

## SAFETY RULES
- NEVER modify auth/token/secret/password/key fields
- NEVER suggest deleting or adding steps
- Maximum delay: 300 seconds
- Only modify fields listed in the step config

## RESPOND WITH JSON ONLY:
{{
  "action": "action_name",
  "confidence": 0.0-1.0,
  "reasoning": "Brief explanation",
  "retry_params": {{"modifications": {{}}, "delay_seconds": 0}},
  "skip_config": {{"output_strategy": "null"}},
  "reroute_target": null
}}"""


def classify_error(error_message: str) -> str:
    """Classify an error into a category."""
    if not error_message:
        return "unknown"
    error_lower = error_message.lower()
    for category, patterns in ERROR_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, error_lower, re.IGNORECASE):
                return category
    return "unknown"


def should_attempt_healing(
    error_category: str,
    attempt: int,
    normal_retries: int,
    max_normal_retries: int,
    healing_attempts: int,
    llm_calls_in_execution: int,
) -> bool:
    """Determine if LLM healing should be attempted."""
    # Don't heal non-healable errors
    if error_category in NON_HEALABLE:
        return False

    # Normal retries must be exhausted first
    if normal_retries < max_normal_retries:
        return False

    # Check healing attempt limits
    if healing_attempts >= MAX_HEALING_ATTEMPTS:
        return False

    # Check per-execution LLM call budget
    if llm_calls_in_execution >= MAX_LLM_CALLS_PER_EXECUTION:
        return False

    return True


def request_healing(
    llm_fn: Callable,
    node_id: str,
    node_type: str,
    node_config: dict,
    error_message: str,
    error_category: str,
    attempt: int,
    max_attempts: int,
    previous_attempts: list,
    reroute_targets: list = None,
) -> HealingDecision:
    """Ask the LLM for a healing decision."""
    prompt = HEALING_PROMPT.format(
        node_id=node_id,
        node_type=node_type,
        error_message=error_message,
        error_category=error_category,
        attempt=attempt,
        max_attempts=max_attempts,
        previous_attempts=json.dumps(previous_attempts) if previous_attempts else "None",
        step_config=json.dumps(node_config, indent=2)[:1000],  # Token budget
        reroute_targets=json.dumps(reroute_targets or []),
    )

    try:
        response = llm_fn(prompt)
        # Strip markdown code fences
        response = response.strip()
        if response.startswith("```"):
            response = re.sub(r"^```\w*\n?", "", response)
            response = re.sub(r"\n?```$", "", response)

        decision_data = json.loads(response)
        return HealingDecision(
            action=decision_data.get("action", "escalate"),
            confidence=float(decision_data.get("confidence", 0.0)),
            reasoning=decision_data.get("reasoning", ""),
            retry_params=decision_data.get("retry_params"),
            skip_config=decision_data.get("skip_config"),
            reroute_target=decision_data.get("reroute_target"),
            step_modifications=decision_data.get("step_modifications"),
        )
    except json.JSONDecodeError:
        return HealingDecision(
            action="escalate",
            confidence=0.0,
            reasoning=f"LLM response was not valid JSON: {response[:200]}",
        )
    except Exception as e:
        return HealingDecision(
            action="escalate",
            confidence=0.0,
            reasoning=f"LLM call failed: {e}",
        )


def validate_decision(decision: HealingDecision, node_config: dict) -> tuple[bool, str]:
    """
    Validate the LLM's healing decision against guardrails.
    Returns (is_valid, rejection_reason).
    """
    valid_actions = {"retry_modified", "skip", "reroute", "escalate", "abort"}
    if decision.action not in valid_actions:
        return False, f"Invalid action: {decision.action}"

    if decision.confidence < ESCALATE_THRESHOLD:
        return False, f"Confidence {decision.confidence} below threshold {ESCALATE_THRESHOLD}"

    # Validate retry modifications
    if decision.action == "retry_modified" and decision.retry_params:
        mods = decision.retry_params.get("modifications", {})
        for key in mods:
            # Check immutable fields
            if any(immutable in key.lower() for immutable in IMMUTABLE_FIELDS):
                return False, f"Cannot modify immutable field: {key}"

        delay = decision.retry_params.get("delay_seconds", 0)
        if delay > 300:
            return False, f"Delay {delay}s exceeds maximum 300s"

    # Validate skip
    if decision.action == "skip":
        criticality = node_config.get("criticality", "required")
        if criticality == "critical":
            return False, "Cannot skip a critical step"

    return True, ""


def apply_healing(
    decision: HealingDecision,
    node_config: dict,
) -> dict:
    """Apply a healing decision to get modified node config."""
    if decision.action == "retry_modified" and decision.retry_params:
        modified = dict(node_config)
        mods = decision.retry_params.get("modifications", {})
        for key, value in mods.items():
            if key in modified.get("config", {}):
                modified["config"][key] = value
            elif key in modified:
                modified[key] = value
        return modified

    return node_config
