"""
Executor module - Runtime engine for durable workflows.

This module contains the execution components:
- executor: Direct flow execution (Executor class)
- outcome: FlowOutcome state machine (Completed/Suspended)
- timer: Durable timer scheduling with Observer pattern
- signal: External signal handling
- dag: DAG-based parallel step execution

From Dave Cheney: "Package Design"
Package name "executor" describes what it provides (execution engine),
not what it contains (executors, timers).

Note: Scheduler and Worker have been moved to top-level ergon module
following Dave Cheney's naming advice:
- ergon.Executor (not ergon.FlowExecutor)
- ergon.Scheduler (not ergon.FlowScheduler)
- ergon.Worker (not ergon.FlowWorker)
"""

from ergon.executor.instance import Executor, execute_flow
from ergon.executor.outcome import (
    SuspendReason,
    Completed,
    Suspended,
    FlowOutcome,
    is_completed,
    is_suspended,
)
from ergon.executor.suspension_payload import SuspensionPayload
from ergon.executor.pending_child import PendingChild
from ergon.executor.child_completion import complete_child_flow
from ergon.executor.timer import schedule_timer, schedule_timer_named
from ergon.executor.signal import await_external_signal, signal_resume
from ergon.executor.dag import StepHandle, DeferredRegistry, DagSummary, execute_dag

__all__ = [
    # Executor
    "Executor",
    "execute_flow",
    # FlowOutcome state machine
    "SuspendReason",
    "Completed",
    "Suspended",
    "FlowOutcome",
    "is_completed",
    "is_suspended",
    "SuspensionPayload",
    # Child flows
    "PendingChild",
    "complete_child_flow",
    # Timers
    "schedule_timer",
    "schedule_timer_named",
    # Signals
    "await_external_signal",
    "signal_resume",
    # DAG
    "StepHandle",
    "DeferredRegistry",
    "DagSummary",
    "execute_dag",
]
