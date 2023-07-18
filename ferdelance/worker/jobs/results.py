from dataclasses import dataclass


@dataclass
class ExecutionResult:
    job_id: str
    path: str
    metrics: list
    is_model: bool = False
    is_estimate: bool = False
