"""Flow execution outcomes and suspension reasons.

Defines the FlowOutcome state machine for flow execution results.
A flow can either complete (successfully or with error) or suspend
(waiting for timer or signal).
"""

from dataclasses import dataclass
from typing import Generic, TypeVar
from uuid import UUID

__all__ = [
    "SuspendReason",
    "Completed",
    "Suspended",
    "FlowOutcome",
    "_SuspendExecution",
]

# Type variable for flow result type
R = TypeVar("R")


# =============================================================================
# Flow Control Signals (Not Errors)
# =============================================================================


class _FlowControl(BaseException):
    """Base class for flow control signals.

    Like StopIteration and GeneratorExit, these are control flow
    mechanisms, not errors. Inheriting from BaseException prevents
    accidental catching by generic exception handlers.
    """

    pass


class _SuspendExecution(_FlowControl):  # noqa: N818
    """Signal that flow execution should suspend.

    Raised by schedule_timer(), await_external_signal(), and
    pending_child.result() to indicate suspension. The Executor
    catches this and checks ctx.take_suspend_reason() for details.

    Internal mechanism - never visible to users. The noqa comment
    suppresses N818 because this is a control flow signal, not an error.
    """

    pass


@dataclass(frozen=True)
class SuspendReason:
    """Reason why a flow suspended execution.

    A flow suspends when waiting for a timer or external signal
    (including child flow completion).
    """

    flow_id: UUID
    step: int
    signal_name: str | None = None

    def is_timer(self) -> bool:
        return self.signal_name is None

    def is_signal(self) -> bool:
        return self.signal_name is not None

    def __str__(self) -> str:
        if self.is_timer():
            return f"Timer(flow_id={self.flow_id}, step={self.step})"
        else:
            return (
                f"Signal(flow_id={self.flow_id}, step={self.step}, "
                f"signal_name={self.signal_name!r})"
            )


@dataclass(frozen=True)
class Completed(Generic[R]):
    """Flow completed execution (success or failure).

    The result can be a success value or an exception.
    """

    result: R

    def is_success(self) -> bool:
        return not isinstance(self.result, BaseException)

    def is_failure(self) -> bool:
        return isinstance(self.result, BaseException)

    def __str__(self) -> str:
        if self.is_success():
            return f"Completed(success={self.result!r})"
        else:
            return f"Completed(error={type(self.result).__name__}: {self.result})"


@dataclass(frozen=True)
class Suspended:
    """Flow suspended, waiting for external event."""

    reason: SuspendReason

    def __str__(self) -> str:
        return f"Suspended({self.reason})"


# Union type representing the result of flow execution
FlowOutcome = Completed[R] | Suspended


# =============================================================================
# TYPE GUARDS FOR FLOW OUTCOME
# =============================================================================


def is_completed(outcome: FlowOutcome[R]) -> bool:
    """Type guard to check if outcome is Completed."""
    return isinstance(outcome, Completed)


def is_suspended(outcome: FlowOutcome[R]) -> bool:
    """Type guard to check if outcome is Suspended."""
    return isinstance(outcome, Suspended)
