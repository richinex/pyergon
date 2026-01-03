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
- pyergon.Executor (not pyergon.FlowExecutor)
- pyergon.Scheduler (not pyergon.FlowScheduler)
- pyergon.Worker (not pyergon.FlowWorker)
"""

from pyergon.executor.child_completion import complete_child_flow
from pyergon.executor.dag import DagSummary, DeferredRegistry, StepHandle, execute_dag
from pyergon.executor.instance import Executor, execute_flow
from pyergon.executor.outcome import (
    Completed,
    FlowOutcome,
    Suspended,
    SuspendReason,
    is_completed,
    is_suspended,
)
from pyergon.executor.pending_child import PendingChild
from pyergon.executor.signal import await_external_signal, signal_resume
from pyergon.executor.suspension_payload import SuspensionPayload
from pyergon.executor.timer import schedule_timer, schedule_timer_named

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
