"""
Pipeline Engine — n8n-style workflow engine with LLM self-healing.

Durable DAG execution, crash recovery, human-in-the-loop,
and LLM-powered auto-healing on failures.
"""

import json
import time
import uuid
import hashlib
import logging
import threading
import concurrent.futures
from typing import Optional, Callable, Any

from pipeline_engine.db import PipelineDB
from pipeline_engine.models import (
    Workflow, Execution, NodeExecution, NodeResult,
    ExecutionStatus, NodeStatus, HealingDecision, Timer,
)
from pipeline_engine.dag import topological_sort, get_ready_nodes, validate_workflow
from pipeline_engine.nodes import get_node_executor, NODE_REGISTRY
from pipeline_engine.expressions import resolve_dict
from pipeline_engine.healing import (
    classify_error, should_attempt_healing, request_healing,
    validate_decision, apply_healing,
    MAX_HEALING_ATTEMPTS, MAX_LLM_CALLS_PER_EXECUTION,
)
from pipeline_engine.errors import (
    ValidationError, WorkflowNotFoundError,
    ExecutionNotFoundError, NodeExecutionError,
)

logger = logging.getLogger("pipeline")

TICK_INTERVAL = 1.0  # seconds


class PipelineEngine:
    """
    Durable workflow engine with LLM self-healing.

    Workflows are JSON recipes with nodes and edges forming a DAG.
    Execution state is persisted to SQLite — survives crashes.
    Failed steps trigger LLM-powered healing when available.
    """

    def __init__(self, config: Optional[dict] = None):
        self.config = config or {}
        self.db = PipelineDB(self.config)
        self._executor_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.config.get("max_workers", 20)
        )
        self._llm_fn: Optional[Callable] = self.config.get("llm_fn")
        self._running = True
        self._tick_timer = None
        self._llm_calls_per_execution: dict[str, int] = {}

        # Recover interrupted executions on startup
        self._recover()

        # Start scheduler
        self._start_tick()

        logger.info("PipelineEngine initialized (db=%s)", self.db.db_path)

    # ── Workflow CRUD ──────────────────────────────────────────────────

    def register_workflow(self, definition: dict) -> str:
        """Register a workflow recipe. Returns workflow_id."""
        errors = validate_workflow(definition)
        if errors:
            raise ValidationError(f"Invalid workflow: {'; '.join(errors)}")

        workflow_id = uuid.uuid4().hex[:24]
        name = definition.get("name", f"workflow-{workflow_id[:8]}")
        version = definition.get("version", "1.0.0")
        desc = definition.get("description", "")
        now = _now()
        checksum = hashlib.sha256(json.dumps(definition, sort_keys=True).encode()).hexdigest()[:16]

        with self.db.cursor() as cur:
            cur.execute(
                """INSERT INTO workflows
                   (id, name, version, description, definition, checksum, is_active, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)""",
                (workflow_id, name, version, desc, json.dumps(definition), checksum, now, now),
            )

        logger.info("Workflow registered: %s (%s v%s)", workflow_id, name, version)
        return workflow_id

    def get_workflow(self, workflow_id: str) -> dict:
        cur = self.db.read_cursor()
        cur.execute("SELECT * FROM workflows WHERE id = ?", (workflow_id,))
        row = cur.fetchone()
        if not row:
            raise WorkflowNotFoundError(f"Workflow {workflow_id} not found")
        return dict(row)

    def list_workflows(self, active_only: bool = True) -> list[dict]:
        cur = self.db.read_cursor()
        if active_only:
            cur.execute("SELECT id, name, version, description, checksum, created_at FROM workflows WHERE is_active = 1")
        else:
            cur.execute("SELECT id, name, version, description, checksum, created_at FROM workflows")
        return [dict(row) for row in cur.fetchall()]

    def delete_workflow(self, workflow_id: str) -> bool:
        with self.db.cursor() as cur:
            cur.execute("UPDATE workflows SET is_active = 0, updated_at = ? WHERE id = ?", (_now(), workflow_id))
            return cur.rowcount > 0

    # ── Execution Lifecycle ────────────────────────────────────────────

    def start_execution(
        self,
        workflow_id: str,
        inputs: Optional[dict] = None,
        trigger_type: str = "manual",
        trigger_data: Optional[dict] = None,
        llm_fn: Optional[Callable] = None,
    ) -> str:
        """Start a new workflow execution. Returns execution_id."""
        workflow = self.get_workflow(workflow_id)
        definition = json.loads(workflow["definition"])

        exec_id = uuid.uuid4().hex[:24]
        now = _now()

        with self.db.cursor() as cur:
            cur.execute(
                """INSERT INTO executions
                   (id, workflow_id, workflow_version, status, trigger_type, trigger_data,
                    inputs, started_at, created_at, updated_at)
                   VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?, ?)""",
                (exec_id, workflow_id, workflow["version"], trigger_type,
                 json.dumps(trigger_data) if trigger_data else None,
                 json.dumps(inputs) if inputs else None, now, now, now),
            )

            # Create node execution entries
            for node_id in definition.get("nodes", {}):
                ne_id = uuid.uuid4().hex[:24]
                cur.execute(
                    """INSERT INTO node_executions
                       (id, execution_id, node_id, status, attempt, created_at, updated_at)
                       VALUES (?, ?, ?, 'pending', 1, ?, ?)""",
                    (ne_id, exec_id, node_id, now, now),
                )

            # Log
            cur.execute(
                "INSERT INTO execution_log (execution_id, event_type, details, created_at) VALUES (?, ?, ?, ?)",
                (exec_id, "execution_started", json.dumps({"trigger": trigger_type}), now),
            )

        # Store LLM function for this execution
        if llm_fn:
            self._llm_fn = llm_fn
        self._llm_calls_per_execution[exec_id] = 0

        # Advance immediately
        self._advance_execution(exec_id, definition)

        logger.info("Execution started: %s (workflow=%s)", exec_id, workflow_id)
        return exec_id

    def cancel_execution(self, execution_id: str) -> bool:
        with self.db.cursor() as cur:
            cur.execute(
                "UPDATE executions SET status = 'cancelled', completed_at = ?, updated_at = ? WHERE id = ? AND status IN ('running', 'paused')",
                (_now(), _now(), execution_id),
            )
            return cur.rowcount > 0

    def pause_execution(self, execution_id: str) -> bool:
        with self.db.cursor() as cur:
            cur.execute(
                "UPDATE executions SET status = 'paused', updated_at = ? WHERE id = ? AND status = 'running'",
                (_now(), execution_id),
            )
            return cur.rowcount > 0

    def resume_execution(self, execution_id: str) -> bool:
        with self.db.cursor() as cur:
            cur.execute(
                "UPDATE executions SET status = 'running', updated_at = ? WHERE id = ? AND status = 'paused'",
                (_now(), execution_id),
            )
            if cur.rowcount > 0:
                exec_row = self.db.read_cursor()
                exec_row.execute("SELECT * FROM executions WHERE id = ?", (execution_id,))
                row = exec_row.fetchone()
                if row:
                    definition = json.loads(
                        self.db.read_cursor().execute(
                            "SELECT definition FROM workflows WHERE id = ?", (row["workflow_id"],)
                        ).fetchone()["definition"]
                    )
                    self._advance_execution(execution_id, definition)
                return True
            return False

    def get_execution(self, execution_id: str) -> dict:
        cur = self.db.read_cursor()
        cur.execute("SELECT * FROM executions WHERE id = ?", (execution_id,))
        row = cur.fetchone()
        if not row:
            raise ExecutionNotFoundError(f"Execution {execution_id} not found")
        result = dict(row)

        # Include node executions
        cur.execute("SELECT * FROM node_executions WHERE execution_id = ? ORDER BY created_at", (execution_id,))
        result["nodes"] = [dict(r) for r in cur.fetchall()]

        return result

    def list_executions(self, workflow_id: Optional[str] = None, status: Optional[str] = None, limit: int = 50) -> list[dict]:
        query = "SELECT * FROM executions WHERE 1=1"
        params: list[Any] = []
        if workflow_id:
            query += " AND workflow_id = ?"
            params.append(workflow_id)
        if status:
            query += " AND status = ?"
            params.append(status)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        cur = self.db.read_cursor()
        cur.execute(query, params)
        return [dict(row) for row in cur.fetchall()]

    # ── Human-in-the-Loop ──────────────────────────────────────────────

    def submit_approval(
        self,
        execution_id: str,
        node_id: str,
        approved: bool,
        data: Optional[dict] = None,
        actor: str = "",
    ) -> bool:
        """Submit an approval decision for a paused workflow step."""
        now = _now()
        with self.db.cursor() as cur:
            cur.execute(
                """UPDATE node_executions SET
                   status = ?, output_data = ?, completed_at = ?, updated_at = ?
                   WHERE execution_id = ? AND node_id = ? AND status = 'waiting'""",
                (
                    "completed" if approved else "failed",
                    json.dumps({"approved": approved, "data": data, "actor": actor}),
                    now, now, execution_id, node_id,
                ),
            )
            if cur.rowcount == 0:
                return False

            cur.execute(
                "INSERT INTO execution_log (execution_id, node_id, event_type, details, created_at) VALUES (?, ?, ?, ?, ?)",
                (execution_id, node_id, "approval_received",
                 json.dumps({"approved": approved, "actor": actor}), now),
            )

        # Resume if execution was paused
        self.resume_execution(execution_id)
        return True

    def get_pending_approvals(self, limit: int = 50) -> list[dict]:
        cur = self.db.read_cursor()
        cur.execute(
            """SELECT ne.*, e.workflow_id FROM node_executions ne
               JOIN executions e ON ne.execution_id = e.id
               WHERE ne.status = 'waiting' ORDER BY ne.created_at LIMIT ?""",
            (limit,),
        )
        return [dict(row) for row in cur.fetchall()]

    # ── Execution Advancement ──────────────────────────────────────────

    def _advance_execution(self, execution_id: str, definition: dict):
        """Advance an execution by running ready nodes."""
        nodes = definition.get("nodes", {})
        edges = definition.get("edges", [])
        inputs = {}

        # Get current node statuses
        cur = self.db.read_cursor()
        cur.execute("SELECT * FROM node_executions WHERE execution_id = ?", (execution_id,))
        node_rows = {row["node_id"]: dict(row) for row in cur.fetchall()}

        node_statuses = {}
        node_outputs = {}
        for nid, row in node_rows.items():
            node_statuses[nid] = row["status"]
            if row["output_data"]:
                try:
                    node_outputs[nid] = json.loads(row["output_data"])
                except json.JSONDecodeError:
                    node_outputs[nid] = {}

        # Get execution inputs
        cur.execute("SELECT inputs FROM executions WHERE id = ?", (execution_id,))
        exec_row = cur.fetchone()
        if exec_row and exec_row["inputs"]:
            try:
                inputs = json.loads(exec_row["inputs"])
            except json.JSONDecodeError:
                pass

        # Check if all nodes are done
        all_done = all(s in ("completed", "failed", "skipped", "healed") for s in node_statuses.values())
        if all_done and node_statuses:
            any_failed = any(s == "failed" for s in node_statuses.values())
            final_status = "failed" if any_failed else "completed"
            with self.db.cursor() as cur:
                cur.execute(
                    "UPDATE executions SET status = ?, completed_at = ?, outputs = ?, updated_at = ? WHERE id = ?",
                    (final_status, _now(), json.dumps(node_outputs), _now(), execution_id),
                )
                cur.execute(
                    "INSERT INTO execution_log (execution_id, event_type, details, created_at) VALUES (?, ?, ?, ?)",
                    (execution_id, "execution_completed", json.dumps({"status": final_status}), _now()),
                )
            return

        # Find ready nodes
        ready = get_ready_nodes(nodes, edges, node_statuses)

        # Build context for expression resolution
        context = {
            "inputs": inputs,
            "nodes": {nid: {"output": node_outputs.get(nid, {})} for nid in nodes},
            "_llm_fn": self._llm_fn,
        }

        # Execute ready nodes
        for node_id in ready:
            node_def = nodes[node_id]
            self._executor_pool.submit(
                self._execute_node, execution_id, node_id, node_def, context, definition
            )

    def _execute_node(self, execution_id: str, node_id: str, node_def: dict, context: dict, definition: dict):
        """Execute a single node with retry and healing."""
        node_type = node_def.get("type", "noop")
        config = node_def.get("config", {})
        max_retries = node_def.get("retry", {}).get("max_attempts", 3)
        now = _now()

        # Resolve config templates
        resolved_config = resolve_dict(config, context)

        executor = get_node_executor(node_type)
        if not executor:
            self._update_node_status(execution_id, node_id, "failed", error=f"Unknown node type: {node_type}")
            return

        # Mark as running
        self._update_node_status(execution_id, node_id, "running")

        # Execute with retries
        result = None
        healing_log = []
        healing_attempts = 0

        for attempt in range(1, max_retries + MAX_HEALING_ATTEMPTS + 1):
            try:
                result = executor.execute(resolved_config, {}, context)

                if result.status == "completed":
                    self._update_node_status(
                        execution_id, node_id,
                        "healed" if healing_attempts > 0 else "completed",
                        output=result.output,
                    )
                    break

                elif result.status == "waiting":
                    self._update_node_status(execution_id, node_id, "waiting")
                    # Create timer for the wait
                    if result.wait_until:
                        with self.db.cursor() as cur:
                            cur.execute(
                                """INSERT INTO timers (id, execution_id, node_id, timer_type, fires_at, status, created_at)
                                   VALUES (?, ?, ?, 'delay', ?, 'pending', ?)""",
                                (uuid.uuid4().hex[:24], execution_id, node_id, result.wait_until, _now()),
                            )
                    # Pause execution
                    with self.db.cursor() as cur:
                        cur.execute(
                            "UPDATE executions SET status = 'paused', updated_at = ? WHERE id = ?",
                            (_now(), execution_id),
                        )
                    return

                elif result.status == "failed":
                    error_msg = result.error or "Unknown error"
                    error_category = classify_error(error_msg)

                    # Normal retry first
                    if attempt <= max_retries:
                        logger.debug("Node %s attempt %d failed: %s (retrying)", node_id, attempt, error_msg)
                        backoff = min(2 ** (attempt - 1), 30)
                        time.sleep(backoff)
                        continue

                    # Try LLM healing
                    if (
                        self._llm_fn
                        and should_attempt_healing(
                            error_category, attempt, attempt - 1, max_retries,
                            healing_attempts,
                            self._llm_calls_per_execution.get(execution_id, 0),
                        )
                    ):
                        healing_attempts += 1
                        self._llm_calls_per_execution[execution_id] = \
                            self._llm_calls_per_execution.get(execution_id, 0) + 1

                        decision = request_healing(
                            llm_fn=self._llm_fn,
                            node_id=node_id,
                            node_type=node_type,
                            node_config=node_def,
                            error_message=error_msg,
                            error_category=error_category,
                            attempt=attempt,
                            max_attempts=max_retries + MAX_HEALING_ATTEMPTS,
                            previous_attempts=healing_log,
                        )

                        # Log healing attempt
                        heal_entry = {
                            "attempt": healing_attempts,
                            "error": error_msg,
                            "category": error_category,
                            "decision": decision.action,
                            "confidence": decision.confidence,
                            "reasoning": decision.reasoning,
                        }
                        healing_log.append(heal_entry)

                        # Persist healing history
                        with self.db.cursor() as cur:
                            cur.execute(
                                """INSERT INTO healing_history
                                   (execution_id, node_id, attempt, error_category, error_message,
                                    decision_action, decision_confidence, decision_reasoning,
                                    decision_raw, outcome, created_at)
                                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                                (execution_id, node_id, healing_attempts, error_category,
                                 error_msg, decision.action, decision.confidence,
                                 decision.reasoning, json.dumps(heal_entry), "pending", _now()),
                            )

                        # Validate decision
                        is_valid, rejection = validate_decision(decision, node_def)
                        if not is_valid:
                            logger.warning("Healing rejected for %s: %s", node_id, rejection)
                            continue

                        # Apply decision
                        if decision.action == "retry_modified":
                            resolved_config = resolve_dict(
                                apply_healing(decision, node_def).get("config", config),
                                context,
                            )
                            delay = (decision.retry_params or {}).get("delay_seconds", 0)
                            if delay > 0:
                                time.sleep(min(delay, 300))
                            continue

                        elif decision.action == "skip":
                            self._update_node_status(execution_id, node_id, "skipped",
                                                     output={"skipped": True, "reason": decision.reasoning})
                            break

                        elif decision.action == "escalate":
                            self._update_node_status(execution_id, node_id, "waiting")
                            with self.db.cursor() as cur:
                                cur.execute(
                                    "UPDATE executions SET status = 'paused', updated_at = ? WHERE id = ?",
                                    (_now(), execution_id),
                                )
                            return

                        elif decision.action == "abort":
                            self._update_node_status(execution_id, node_id, "failed", error=error_msg)
                            with self.db.cursor() as cur:
                                cur.execute(
                                    "UPDATE executions SET status = 'failed', error = ?, completed_at = ?, updated_at = ? WHERE id = ?",
                                    (f"Aborted by healer: {decision.reasoning}", _now(), _now(), execution_id),
                                )
                            return

                    # No healing available — fail
                    self._update_node_status(execution_id, node_id, "failed",
                                             error=error_msg, healing_log=healing_log)

                    on_error = definition.get("on_error", "fail")
                    if on_error == "fail":
                        with self.db.cursor() as cur:
                            cur.execute(
                                "UPDATE executions SET status = 'failed', error = ?, completed_at = ?, updated_at = ? WHERE id = ?",
                                (error_msg, _now(), _now(), execution_id),
                            )
                    elif on_error == "pause":
                        with self.db.cursor() as cur:
                            cur.execute(
                                "UPDATE executions SET status = 'paused', updated_at = ? WHERE id = ?",
                                (_now(), execution_id),
                            )
                    return

            except Exception as e:
                logger.error("Node %s execution error: %s", node_id, e)
                if attempt >= max_retries:
                    self._update_node_status(execution_id, node_id, "failed", error=str(e))
                    return

        # After node completes, advance the DAG
        self._advance_execution(execution_id, definition)

    def _update_node_status(
        self,
        execution_id: str,
        node_id: str,
        status: str,
        output: Optional[dict] = None,
        error: Optional[str] = None,
        healing_log: Optional[list] = None,
    ):
        now = _now()
        with self.db.cursor() as cur:
            cur.execute(
                """UPDATE node_executions SET
                   status = ?, output_data = ?, error = ?, healing_log = ?,
                   completed_at = CASE WHEN ? IN ('completed','failed','skipped','healed') THEN ? ELSE completed_at END,
                   started_at = CASE WHEN started_at IS NULL AND ? = 'running' THEN ? ELSE started_at END,
                   updated_at = ?
                   WHERE execution_id = ? AND node_id = ?""",
                (status, json.dumps(output) if output else None, error,
                 json.dumps(healing_log) if healing_log else None,
                 status, now, status, now, now, execution_id, node_id),
            )
            cur.execute(
                "INSERT INTO execution_log (execution_id, node_id, event_type, details, created_at) VALUES (?, ?, ?, ?, ?)",
                (execution_id, node_id, f"node.{status}",
                 json.dumps({"error": error} if error else {"output_keys": list((output or {}).keys())}), now),
            )

    # ── Scheduler ──────────────────────────────────────────────────────

    def _start_tick(self):
        if not self._running:
            return
        self._tick_timer = threading.Timer(TICK_INTERVAL, self._tick)
        self._tick_timer.daemon = True
        self._tick_timer.start()

    def _tick(self):
        """One scheduler tick: fire timers, advance paused executions."""
        try:
            self._fire_timers()
        except Exception as e:
            logger.error("Tick error: %s", e)
        if self._running:
            self._start_tick()

    def _fire_timers(self):
        now = _now()
        cur = self.db.read_cursor()
        cur.execute(
            "SELECT * FROM timers WHERE status = 'pending' AND fires_at <= ?",
            (now,),
        )
        for row in cur.fetchall():
            with self.db.cursor() as wc:
                wc.execute("UPDATE timers SET status = 'fired' WHERE id = ?", (row["id"],))

            if row["execution_id"] and row["node_id"]:
                # Resume the node/execution
                self._update_node_status(row["execution_id"], row["node_id"], "completed",
                                         output={"timer_fired": True})
                self.resume_execution(row["execution_id"])

    # ── Recovery ───────────────────────────────────────────────────────

    def _recover(self):
        """Recover interrupted executions on startup."""
        cur = self.db.read_cursor()
        cur.execute("SELECT id, workflow_id FROM executions WHERE status = 'running'")
        for row in cur.fetchall():
            logger.info("Recovering execution: %s", row["id"])
            try:
                wf = self.db.read_cursor()
                wf.execute("SELECT definition FROM workflows WHERE id = ?", (row["workflow_id"],))
                wf_row = wf.fetchone()
                if wf_row:
                    definition = json.loads(wf_row["definition"])
                    self._advance_execution(row["id"], definition)
            except Exception as e:
                logger.error("Recovery failed for %s: %s", row["id"], e)

    # ── Stats & Health ─────────────────────────────────────────────────

    def stats(self) -> dict:
        cur = self.db.read_cursor()

        cur.execute("SELECT COUNT(*) as c FROM workflows WHERE is_active = 1")
        workflows = cur.fetchone()["c"]

        cur.execute("SELECT status, COUNT(*) as c FROM executions GROUP BY status")
        exec_by_status = {row["status"]: row["c"] for row in cur.fetchall()}

        cur.execute("SELECT COUNT(*) as c FROM node_executions WHERE status = 'waiting'")
        pending_approvals = cur.fetchone()["c"]

        cur.execute("SELECT COUNT(*) as c FROM healing_history")
        healing_total = cur.fetchone()["c"]

        cur.execute("SELECT COUNT(*) as c FROM healing_history WHERE outcome = 'success'")
        healing_success = cur.fetchone()["c"]

        return {
            "active_workflows": workflows,
            "executions_by_status": exec_by_status,
            "pending_approvals": pending_approvals,
            "healing_attempts": healing_total,
            "healing_successes": healing_success,
            "node_types_available": list(NODE_REGISTRY.keys()),
        }

    def health_check(self) -> dict:
        try:
            stats = self.stats()
            return {"healthy": True, "engine": "pipeline", **stats}
        except Exception as e:
            return {"healthy": False, "engine": "pipeline", "error": str(e)}

    def get_healing_history(self, execution_id: Optional[str] = None, limit: int = 50) -> list[dict]:
        cur = self.db.read_cursor()
        if execution_id:
            cur.execute(
                "SELECT * FROM healing_history WHERE execution_id = ? ORDER BY created_at DESC LIMIT ?",
                (execution_id, limit),
            )
        else:
            cur.execute("SELECT * FROM healing_history ORDER BY created_at DESC LIMIT ?", (limit,))
        return [dict(row) for row in cur.fetchall()]

    # ── Lifecycle ──────────────────────────────────────────────────────

    def close(self):
        self._running = False
        if self._tick_timer:
            self._tick_timer.cancel()
        self._executor_pool.shutdown(wait=False)
        self.db.close()
        logger.info("PipelineEngine closed")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + f".{int(time.time() * 1000) % 1000:03d}"
