"""
Ergon: Durable Execution Framework for Python

Pure Python implementation of durable execution with Temporal-like semantics.

Design Pattern: Façade Pattern (Chapter 10)
This module provides a simplified interface to the ergon framework,
hiding the complexity of storage, execution, and timer coordination.

From Dave Cheney: "A good package starts with its name"
Package "ergon" (Greek: work/action) describes what it provides.

Example:
    ```python
    import asyncio
    from dataclasses import dataclass
    from pyergon import flow, flow_type, step, SqliteExecutionLog

    @dataclass
    @flow_type
    class MyWorkflow:
        @step
        async def fetch_data(self):
            # This step is cached and retried on failure
            return await some_api_call()

        @step
        async def process_data(self, data):
            return transform(data)

        @flow
        async def run(self):
            data = await self.fetch_data()
            return await self.process_data(data)

    # Execute with durable storage
    async def main():
        storage = SqliteExecutionLog("workflow.db")
        await storage.connect()

        workflow = MyWorkflow()
        result = await workflow.run()

        await storage.close()

    asyncio.run(main())
    ```
"""

# Core types - Pure Python implementations
from pyergon.core import (
    Invocation,
    InvocationStatus,
    CallType,
    Context,
    FlowType,
    TaskStatus,
    ScheduledFlow,
    RetryPolicy,
    RetryableError,
)

# Storage - Pure Python implementations (Adapter pattern)
from pyergon.storage import ExecutionLog, SqliteExecutionLog, InMemoryExecutionLog

# Decorators - Pure Python
from pyergon.decorators import flow, flow_type, step

# Execution - Pure Python (Template Method + Strategy patterns)
# Following Dave Cheney: "The name of an identifier includes its package name"
# pyergon.Executor, pyergon.Scheduler, pyergon.Worker (no Flow prefix needed)
from pyergon.executor.instance import Executor, execute_flow
from pyergon.executor.outcome import (
    SuspendReason,
    Completed,
    Suspended,
    FlowOutcome,
    is_completed,
    is_suspended,
)
from pyergon.executor.suspension_payload import SuspensionPayload
from pyergon.executor.pending_child import PendingChild
from pyergon.executor.child_completion import complete_child_flow
from pyergon.executor.scheduler import Scheduler, SchedulerError
from pyergon.executor.worker import Worker, WorkerHandle, WorkerError

# Timers - Pure Python
from pyergon.executor import schedule_timer, schedule_timer_named

# Signals - Pure Python (External event coordination)
from pyergon.executor import await_external_signal, signal_resume

# DAG execution - Pure Python (Parallel step execution with dependencies)
from pyergon.executor import StepHandle, DeferredRegistry, DagSummary, execute_dag
from pyergon.executor.dag_runtime import dag, DagExecutionError

# Version
__version__ = "0.1.0"

__all__ = [
    # Core types
    "Invocation",
    "InvocationStatus",
    "CallType",
    "Context",
    "FlowType",
    "TaskStatus",
    "ScheduledFlow",
    "RetryPolicy",
    "RetryableError",

    # Storage (Adapter pattern)
    "ExecutionLog",
    "SqliteExecutionLog",
    "InMemoryExecutionLog",

    # Decorators
    "flow",
    "flow_type",
    "step",

    # Execution (Following Dave Cheney's naming advice)
    "Executor",
    "execute_flow",
    "FlowOutcome",
    "Completed",
    "Suspended",
    "SuspendReason",
    "is_completed",
    "is_suspended",
    "SuspensionPayload",
    "PendingChild",
    "complete_child_flow",

    # Scheduler (matches Python stdlib pattern)
    "Scheduler",
    "SchedulerError",

    # Worker (matches Python stdlib pattern)
    "Worker",
    "WorkerHandle",
    "WorkerError",

    # Timers
    "schedule_timer",
    "schedule_timer_named",

    # Signals (External event coordination)
    "await_external_signal",
    "signal_resume",

    # DAG execution (Parallel step execution)
    "StepHandle",
    "DeferredRegistry",
    "DagSummary",
    "execute_dag",
    "dag",
    "DagExecutionError",

    # Metadata
    "__version__",
]
