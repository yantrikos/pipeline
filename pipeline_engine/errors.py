"""Pipeline error hierarchy."""

from typing import Optional


class PipelineError(Exception):
    code: str = "PIPELINE_ERROR"
    retryable: bool = False

    def __init__(self, message: str, details: Optional[dict] = None):
        super().__init__(message)
        self.details = details or {}


class ValidationError(PipelineError):
    code = "PIPELINE_VALIDATION_ERROR"


class CycleDetectedError(ValidationError):
    code = "PIPELINE_CYCLE_DETECTED"


class NodeExecutionError(PipelineError):
    code = "PIPELINE_NODE_EXECUTION_ERROR"
    retryable = True


class WorkflowNotFoundError(PipelineError):
    code = "PIPELINE_WORKFLOW_NOT_FOUND"


class ExecutionNotFoundError(PipelineError):
    code = "PIPELINE_EXECUTION_NOT_FOUND"


class HealingError(PipelineError):
    code = "PIPELINE_HEALING_ERROR"


class HealingLimitExceededError(HealingError):
    code = "PIPELINE_HEALING_LIMIT_EXCEEDED"


class ApprovalTimeoutError(PipelineError):
    code = "PIPELINE_APPROVAL_TIMEOUT"


class StorageError(PipelineError):
    code = "PIPELINE_STORAGE_ERROR"
    retryable = True
