"""
Flow execution outcomes and suspension reasons.

This module defines the FlowOutcome state machine for flow execution results.

**Rust Reference**:
`/home/richinex/Documents/devs/rust_projects/ergon/ergon/src/executor/error.rs` lines 69-92

**Python Documentation**:
- Dataclasses: https://docs.python.org/3/library/dataclasses.html
- Generic types: https://docs.python.org/3/library/typing.html#typing.Generic
- Union types: https://docs.python.org/3/library/typing.html#typing.Union

**Design Pattern**: State Machine using Union types (BEST_PRACTICES.md section on state machines)

From Dave Cheney's principle: "If your function can suspend, you must tell the caller."
FlowOutcome makes suspension explicit instead of hiding it in exceptions or timeouts.

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
    """
    Base class for flow control signals.

    Like Python's StopIteration and GeneratorExit, these are control flow
    mechanisms, not errors. They inherit from BaseException (not Exception)
    to ensure they're not accidentally caught by generic exception handlers.

    **Design Rationale**:
    - Inheriting from BaseException prevents accidental catching by `except Exception:`
    - Makes it explicit that these are control flow, not error conditions
    - Follows Python's precedent for control flow exceptions

    **Reference**: PEP 352 - Required Superclass for Exceptions
    https://www.python.org/dev/peps/pep-0352/
    """

    pass


class _SuspendExecution(_FlowControl):  # noqa: N818
    """
    Signal that flow execution should suspend (not an error).

    **Python-specific pattern**: In Rust, futures can return Poll::Pending
    without completing. In Python, async functions must complete or raise.

    This signal is raised by schedule_timer(), await_external_signal(), and
    pending_child.result() to indicate the flow should suspend. The Executor
    catches this and checks ctx.take_suspend_reason() for suspension details.

    **Not an Error**: This is control flow, like StopIteration. The noqa comment
    suppresses N818 (exceptions should end with Error) because this is intentionally
    not an error - it's a control flow signal.

    **Visibility**: Internal mechanism, never visible to user code - caught by
    the Executor before returning to the worker.

    **Rust Equivalent**: Poll::Pending in async fn
    """

    pass


@dataclass(frozen=True)
class SuspendReason:
    """
    Reason why a flow suspended execution.

    **Rust Reference**: `src/executor/error.rs` lines 69-80

    A flow suspends when it needs to wait for an external event:
    - Timer: Waiting for a duration to elapse
    - Signal: Waiting for external signal (including child flow completion)

    Attributes:
        flow_id: UUID of the suspended flow
        step: Step number where suspension occurred
        signal_name: Name of signal if waiting for signal, None if waiting for timer

    **Python Best Practice**: Using frozen dataclass for immutability
    Reference: https://docs.python.org/3/library/dataclasses.html#frozen-instances
    """

    flow_id: UUID
    step: int
    signal_name: str | None = None

    def is_timer(self) -> bool:
        """
        Check if this is a timer suspension.

        Returns:
            True if waiting for timer, False if waiting for signal

        Example:
            ```python
            if reason.is_timer():
                print(f"Waiting for timer at step {reason.step}")
            ```
        """
        return self.signal_name is None

    def is_signal(self) -> bool:
        """
        Check if this is a signal suspension.

        Returns:
            True if waiting for signal, False if waiting for timer

        Example:
            ```python
            if reason.is_signal():
                print(f"Waiting for signal '{reason.signal_name}'")
            ```
        """
        return self.signal_name is not None

    def __str__(self) -> str:
        """
        Human-readable string representation.

        **Python Best Practice**: Implement __str__ for user-facing output
        Reference: https://docs.python.org/3/reference/datamodel.html#object.__str__
        """
        if self.is_timer():
            return f"Timer(flow_id={self.flow_id}, step={self.step})"
        else:
            return (
                f"Signal(flow_id={self.flow_id}, step={self.step}, "
                f"signal_name={self.signal_name!r})"
            )


@dataclass(frozen=True)
class Completed(Generic[R]):
    """
    Flow completed execution (success or failure).

    **Rust Reference**: `FlowOutcome::Completed(R)` in `src/executor/error.rs` line 89

    The result can be a success value or an exception. The caller must check
    whether execution succeeded or failed by inspecting the result.

    Attributes:
        result: The flow's return value (success) or raised exception (failure)

    **Python Best Practice**: Using Generic[R] for type parameterization
    Reference: https://docs.python.org/3/library/typing.html#typing.Generic

    Example:
        ```python
        outcome = Completed(result="Order processed")
        # or
        outcome = Completed(result=ValueError("Invalid order"))
        ```
    """

    result: R

    def is_success(self) -> bool:
        """
        Check if flow completed successfully.

        Returns:
            True if result is not an exception, False if result is an exception

        Example:
            ```python
            if outcome.is_success():
                print(f"Success: {outcome.result}")
            else:
                print(f"Failed: {outcome.result}")
            ```
        """
        return not isinstance(self.result, BaseException)

    def is_failure(self) -> bool:
        """
        Check if flow completed with failure.

        Returns:
            True if result is an exception, False otherwise

        Example:
            ```python
            if outcome.is_failure():
                raise outcome.result
            ```
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
    """
    Flow suspended, waiting for external event.

    **Rust Reference**: `FlowOutcome::Suspended(SuspendReason)` in `src/executor/error.rs` line 91

    The flow has paused execution and is waiting for:
    - A timer to fire
    - An external signal to arrive

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


# =============================================================================
# FLOW OUTCOME UNION TYPE
# =============================================================================

# FlowOutcome is a Union type representing the result of flow execution.
# This is equivalent to Rust's enum FlowOutcome<R>.
#
# Python Best Practice: Union types for state machines
# Reference: BEST_PRACTICES.md section "State Machines with Union Types"
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
#         # outcome.result is available here
#         print(outcome.result)
#     elif isinstance(outcome, Suspended):
#         # outcome.reason is available here
#         print(outcome.reason)
#
FlowOutcome = Completed[R] | Suspended


# =============================================================================
# TYPE GUARDS FOR FLOW OUTCOME
# =============================================================================


def is_completed(outcome: FlowOutcome[R]) -> bool:
    """
    Type guard to check if outcome is Completed.

    **Python Best Practice**: Type guards for Union type narrowing
    Reference: https://docs.python.org/3/library/typing.html#typing.TypeGuard

    Args:
        outcome: Flow execution outcome

    Returns:
        True if outcome is Completed, False if Suspended

    Example:
        ```python
        if is_completed(outcome):
            # Type checker knows outcome is Completed[R] here
            print(outcome.result)
        ```
    """
    return isinstance(outcome, Completed)


def is_suspended(outcome: FlowOutcome[R]) -> bool:
    """
    Type guard to check if outcome is Suspended.

    Args:
        outcome: Flow execution outcome

    Returns:
        True if outcome is Suspended, False if Completed

    Example:
        ```python
        if is_suspended(outcome):
            # Type checker knows outcome is Suspended here
            print(outcome.reason)
        ```
    """
    return isinstance(outcome, Suspended)
