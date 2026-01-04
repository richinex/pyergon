"""Execution mode enum for step invocations.

Tracks whether a step is executing for the first time, waiting
for an event, or resuming after suspension.
"""

from enum import Enum


class CallType(Enum):
    """Execution mode for step invocations.

    Distinguishes between first execution, waiting state, and resumption
    after suspension. Used by Context to track execution mode in
    task-local storage.

    Example:
        ```python
        from pyergon.core import CALL_TYPE, CallType

        # Set call type in task-local context
        CALL_TYPE.set(CallType.RUN)

        # Check current call type
        if CALL_TYPE.get() == CallType.AWAIT:
            # Step is waiting for event
            pass
        ```
    """

    RUN = "run"
    """First execution or retry attempt.

    Step has not been executed before, or is being retried after failure.
    """

    AWAIT = "await"
    """Step is waiting for external event (signal or timer).

    Step function returned None to indicate suspension. Flow will be
    marked WAITING_FOR_SIGNAL or WAITING_FOR_TIMER and paused until
    the event occurs.
    """

    RESUME = "resume"
    """Step is resuming after suspension.

    External signal or timer has fired, and step is re-executing
    from where it left off with resume parameters.
    """

    def __str__(self) -> str:
        """String representation for logging."""
        return self.value

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return f"CallType.{self.name}"
