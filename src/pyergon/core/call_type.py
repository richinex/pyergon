"""
Call type enum for step execution modes.

This module defines CallType which distinguishes between different execution modes:
- Run: First execution of a step
- Await: Step is waiting (returned None)
- Resume: Step is resuming after wait

RUST COMPLIANCE: Matches Rust ergon src/core/invocation.rs:43-48

From Rust:
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum CallType {
        Run,    // First execution
        Await,  // Waiting for signal/timer
        Resume, // Resuming from wait
    }

Design: Simple enum with clear semantics, used by Context
to track execution mode in task-local storage.
"""

from enum import Enum


class CallType(Enum):
    """
    Execution mode for a step invocation.

    This enum tracks whether a step is being executed for the first time,
    is in a waiting state, or is resuming after waiting.

    Values:
        RUN: First execution of the step (normal case)
        AWAIT: Step returned None, waiting for external event
        RESUME: Step is resuming after signal or timer

    From Rust ergon src/core/invocation.rs:
        pub enum CallType { Run, Await, Resume }

    Usage:
        # Set call type in task-local context
        CALL_TYPE.set(CallType.RUN)

        # Check call type
        call_type = CALL_TYPE.get()
        if call_type == CallType.AWAIT:
            # Handle waiting case
            pass
    """

    RUN = "run"
    """First execution of a step.

    This is the normal case - step has not been executed before,
    or is being retried after failure.
    """

    AWAIT = "await"
    """Step is waiting for external event.

    Step function returned None, indicating it wants to wait
    for an external signal or timer before continuing.

    The step will be marked as WAITING_FOR_SIGNAL or WAITING_FOR_TIMER
    in storage and execution will pause.
    """

    RESUME = "resume"
    """Step is resuming after waiting.

    The external signal/timer has fired and the step is being
    re-executed with the resume parameters.

    This allows steps to continue from where they left off.
    """

    def __str__(self) -> str:
        """String representation for logging."""
        return self.value

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return f"CallType.{self.name}"
