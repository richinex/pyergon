"""Flow execution outcomes and suspension reasons.

Defines the FlowOutcome state machine for flow execution results.
A flow can either complete (successfully or with error) or suspend
(waiting for timer or signal).

Design: State Machine with Union Types
FlowOutcome makes suspension explicit in the type system instead
of hiding it in exceptions or timeouts. Callers must handle both
completion and suspension cases.

Example:
    ```python
    outcome = await executor.execute(lambda f: f.run())

    match outcome:
        case Completed(result):
            print(f"Flow completed: {result}")
        case Suspended(reason):
            print(f"Flow suspended: {reason}")
    ```
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
    mechanisms, not errors. Inheriting from BaseException (not Exception)
    prevents accidental catching by generic exception handlers.

    Design: Control Flow Exceptions
    Follows Python's precedent for control flow exceptions that
    should not be caught by `except Exception:` handlers.
    """

    pass


class _SuspendExecution(_FlowControl):  # noqa: N818
    """Signal that flow execution should suspend.

    Raised by schedule_timer(), await_external_signal(), and
    pending_child.result() to indicate suspension. The Executor
    catches this and checks ctx.take_suspend_reason() for details.

    Design: Python Suspension Mechanism
    In Python, async functions must complete or raise (unlike futures
    that can return pending). This exception provides control flow
    for suspension without being an error.

    Note: Internal mechanism caught by Executor, never visible to users.
    The noqa comment suppresses N818 (exceptions should end with Error)
    because this is intentionally a control flow signal, not an error.
    """

    pass


@dataclass(frozen=True)
class SuspendReason:
    """Reason why a flow suspended execution.

    A flow suspends when waiting for an external event:
    - Timer: Waiting for a duration to elapse
    - Signal: Waiting for external signal (including child flow completion)

    Attributes:
        flow_id: UUID of the suspended flow
        step: Step number where suspension occurred
        signal_name: Name of signal if waiting for signal, None for timer
    """

    flow_id: UUID
    step: int
    signal_name: str | None = None

    def is_timer(self) -> bool:
        """Check if this is a timer suspension.

        Returns:
            True if waiting for timer, False if waiting for signal
        """
        return self.signal_name is None

    def is_signal(self) -> bool:
        """Check if this is a signal suspension.

        Returns:
            True if waiting for signal, False if waiting for timer
        """
        return self.signal_name is not None

    def __str__(self) -> str:
        """Return human-readable string representation."""
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

    The result can be a success value or an exception. Callers must
    check whether execution succeeded or failed by inspecting the result.

    Attributes:
        result: Flow return value (success) or raised exception (failure)

    Example:
        ```python
        outcome = Completed(result="Order processed")
        # or
        outcome = Completed(result=ValueError("Invalid order"))
        ```
    """

    result: R

    def is_success(self) -> bool:
        """Check if flow completed successfully.

        Returns:
            True if result is not an exception, False otherwise
        """
        return not isinstance(self.result, BaseException)

    def is_failure(self) -> bool:
        """Check if flow completed with failure.

        Returns:
            True if result is an exception, False otherwise
        """
        return isinstance(self.result, BaseException)

    def __str__(self) -> str:
        """Human-readable string representation."""
        if self.is_success():
            return f"Completed(success={self.result!r})"
        else:
            return f"Completed(error={type(self.result).__name__}: {self.result})"


@dataclass(frozen=True)
class Suspended:
    """Flow suspended, waiting for external event.

    The flow has paused execution and is waiting for a timer
    to fire or an external signal to arrive.

    Attributes:
        reason: Why the flow suspended (timer or signal)

    Example:
        ```python
        reason = SuspendReason(
            flow_id=flow_id,
            step=5,
            signal_name="payment_confirmed"
        )
        outcome = Suspended(reason=reason)
        ```
    """

    reason: SuspendReason

    def __str__(self) -> str:
        """Human-readable string representation."""
        return f"Suspended({self.reason})"


# FlowOutcome is a Union type representing the result of flow execution.
#
# Pattern matching (Python 3.10+):
#     match outcome:
#         case Completed(result):
#             handle_result(result)
#         case Suspended(reason):
#             handle_suspension(reason)
#
# Type narrowing:
#     if isinstance(outcome, Completed):
#         print(outcome.result)
#     elif isinstance(outcome, Suspended):
#         print(outcome.reason)
#
FlowOutcome = Completed[R] | Suspended


# =============================================================================
# TYPE GUARDS FOR FLOW OUTCOME
# =============================================================================


def is_completed(outcome: FlowOutcome[R]) -> bool:
    """Type guard to check if outcome is Completed.

    Args:
        outcome: Flow execution outcome

    Returns:
        True if outcome is Completed, False if Suspended

    Example:
        ```python
        if is_completed(outcome):
            print(outcome.result)
        ```
    """
    return isinstance(outcome, Completed)


def is_suspended(outcome: FlowOutcome[R]) -> bool:
    """Type guard to check if outcome is Suspended.

    Args:
        outcome: Flow execution outcome

    Returns:
        True if outcome is Suspended, False if Completed

    Example:
        ```python
        if is_suspended(outcome):
            print(outcome.reason)
        ```
    """
    return isinstance(outcome, Suspended)
