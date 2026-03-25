"""Pipeline data models."""

from dataclasses import dataclass, field
from typing import Optional, Any
from enum import Enum


class ExecutionStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class NodeStatus(Enum):
    PENDING = "pending"
    WAITING = "waiting"  # waiting for timer or approval
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    HEALED = "healed"  # failed then self-healed


@dataclass
class Workflow:
    id: str = ""
    name: str = ""
    version: str = "1.0.0"
    description: str = ""
    definition: dict = field(default_factory=dict)
    checksum: str = ""
    is_active: bool = True
    created_at: str = ""


@dataclass
class Execution:
    id: str = ""
    workflow_id: str = ""
    workflow_version: str = ""
    status: str = "pending"
    trigger_type: str = "manual"
    trigger_data: Optional[dict] = None
    inputs: Optional[dict] = None
    outputs: Optional[dict] = None
    error: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    created_at: str = ""
    correlation_id: Optional[str] = None


@dataclass
class NodeExecution:
    id: str = ""
    execution_id: str = ""
    node_id: str = ""
    status: str = "pending"
    attempt: int = 1
    input_data: Optional[dict] = None
    output_data: Optional[dict] = None
    error: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    healing_log: Optional[list] = None


@dataclass
class NodeResult:
    output: dict = field(default_factory=dict)
    status: str = "completed"  # completed, failed, waiting
    error: Optional[str] = None
    wait_until: Optional[str] = None  # ISO 8601 for delays/approvals


@dataclass
class HealingDecision:
    action: str = "escalate"  # retry_modified, skip, reroute, modify_next, escalate, abort
    confidence: float = 0.0
    reasoning: str = ""
    retry_params: Optional[dict] = None
    skip_config: Optional[dict] = None
    reroute_target: Optional[str] = None
    step_modifications: Optional[list] = None


@dataclass
class Timer:
    id: str = ""
    execution_id: Optional[str] = None
    node_id: Optional[str] = None
    timer_type: str = ""  # delay, cron, sla, approval_timeout
    fires_at: str = ""
    payload: Optional[dict] = None
    status: str = "pending"  # pending, fired, cancelled
    workflow_id: Optional[str] = None
