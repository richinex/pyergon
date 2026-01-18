"""Execution mode enum for step invocations.

Tracks whether a step is executing for the first time, waiting
for an event, or resuming after suspension.
"""

from enum import Enum


class CallType(Enum):
    """Execution mode for step invocations.

    Distinguishes between first execution, waiting state, and resumption
    after suspension.
    """

    RUN = "run"
    """First execution or retry attempt."""

    AWAIT = "await"
    """Step is waiting for external event (signal or timer)."""

    RESUME = "resume"
    """Step is resuming after suspension."""

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return f"CallType.{self.name}"
