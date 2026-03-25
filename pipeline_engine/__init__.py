"""Pipeline — n8n-style workflow engine with LLM self-healing for OpenClaw."""

__version__ = "0.1.0"

from pipeline_engine.engine import PipelineEngine
from pipeline_engine.models import (
    Workflow, Execution, NodeExecution, NodeResult,
    HealingDecision, ExecutionStatus, NodeStatus,
)
from pipeline_engine.errors import (
    PipelineError, ValidationError, CycleDetectedError,
    NodeExecutionError, WorkflowNotFoundError,
    HealingError, HealingLimitExceededError,
)
from pipeline_engine.nodes import NODE_REGISTRY, register_node, BaseNode
