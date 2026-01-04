"""Runtime engine for durable workflows.

Components:
- Executor: Direct flow execution with replay and caching
- FlowOutcome: State machine for flow completion and suspension
- Timer: Durable timer scheduling that survives restarts
- Signal: External event handling and flow resumption
- DAG: Parallel step execution with dependency tracking

Design: Package Naming
Package name describes what it provides (execution engine)
rather than implementation details. Public classes use simple
names (Executor, Scheduler, Worker) without redundant prefixes.
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
