"""
Retry policy configuration for step execution.

Design Pattern: Strategy Pattern
RetryPolicy encapsulates retry behavior, allowing different retry strategies
without modifying the step execution code.

From Rust ergon: src/core/retry.rs

Design Rationale (from Rust version):
- Safe default: no automatic retries (backward compatible)
- Simple retry: max_attempts=3 with standard backoff
- Advanced control: Custom RetryPolicy for full control
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast


@dataclass(frozen=True)
class RetryPolicy:
    """
    Configuration for step retry behavior.

    Controls how many times a step should retry on transient errors and
    the backoff strategy between attempts.

    Examples:
        # Simple: just specify max attempts (uses standard delays)
        policy = RetryPolicy.with_max_attempts(3)

        # Named policy: predefined sensible defaults
        policy = RetryPolicy.STANDARD

        # Custom policy: full control
        policy = RetryPolicy(
            max_attempts=5,
            initial_delay_ms=1000,
            max_delay_ms=30000,
            backoff_multiplier=2.0
        )

    From Rust:
        pub struct RetryPolicy {
            pub max_attempts: u32,
            pub initial_delay: Duration,
            pub max_delay: Duration,
            pub backoff_multiplier: f64,
        }
    """

    max_attempts: int
    """Maximum number of attempts (including the first try).

    For example, max_attempts = 3 means:
    - Attempt 1: immediate (first try)
    - Attempt 2: after initial_delay
    - Attempt 3: after initial_delay * backoff_multiplier

    Default: 1 (no retries, backward compatible)
    """

    initial_delay_ms: int
    """Initial delay before the first retry in milliseconds.

    Default: 1000 (1 second)
    """

    max_delay_ms: int
    """Maximum delay between retries in milliseconds (caps exponential backoff).

    Default: 60000 (60 seconds)
    """

    backoff_multiplier: float
    """Multiplier for exponential backoff.

    Each retry delay is calculated as:
    min(initial_delay * backoff_multiplier^(attempt-1), max_delay)

    Default: 2.0 (doubles each time)
    """

    # =========================================================================
    # Predefined Policies
    # =========================================================================

    if TYPE_CHECKING:
        # Type checker sees these as RetryPolicy
        NONE: RetryPolicy
        STANDARD: RetryPolicy
        AGGRESSIVE: RetryPolicy
    else:
        # Runtime sees these as None (set after class definition)
        NONE = cast("RetryPolicy", None)
        STANDARD = cast("RetryPolicy", None)
        AGGRESSIVE = cast("RetryPolicy", None)

    @classmethod
    def with_max_attempts(cls, max_attempts: int) -> RetryPolicy:
        """
        Create a policy with custom max_attempts (uses standard delays).

        This is the shorthand equivalent to Rust's: retry = 3

        Args:
            max_attempts: Maximum number of attempts

        Returns:
            RetryPolicy with standard delays

        Example:
            policy = RetryPolicy.with_max_attempts(5)
        """
        return cls(
            max_attempts=max_attempts,
            initial_delay_ms=1000,
            max_delay_ms=30000,
            backoff_multiplier=2.0,
        )

    def delay_for_attempt(self, attempt: int) -> int | None:
        """
        Calculate the delay before the next retry attempt.

        Uses exponential backoff: initial_delay * backoff_multiplier^(attempt-1)
        capped at max_delay.

        Args:
            attempt: The current attempt number (1-indexed)

        Returns:
            Delay in milliseconds before the next retry, or None if no more retries.

        Example:
            policy = RetryPolicy.STANDARD
            delay1 = policy.delay_for_attempt(1)  # Returns 1000 (1s)
            delay2 = policy.delay_for_attempt(2)  # Returns 2000 (2s)
            delay3 = policy.delay_for_attempt(3)  # Returns None (max attempts)
        """
        if attempt >= self.max_attempts:
            return None  # No more retries

        # Calculate: initial_delay * multiplier^(attempt-1)
        # attempt=1 (first retry): multiplier^0 = 1 → initial_delay
        # attempt=2 (second retry): multiplier^1 → initial_delay * multiplier
        # attempt=3 (third retry): multiplier^2 → initial_delay * multiplier^2
        exponent = attempt - 1
        multiplier = self.backoff_multiplier**exponent
        delay_ms = self.initial_delay_ms * multiplier

        # Cap at max_delay
        delay_ms = min(delay_ms, self.max_delay_ms)

        return int(delay_ms)

    def __repr__(self) -> str:
        """Readable representation for debugging."""
        return (
            f"RetryPolicy(max_attempts={self.max_attempts}, "
            f"initial_delay_ms={self.initial_delay_ms}, "
            f"max_delay_ms={self.max_delay_ms}, "
            f"backoff_multiplier={self.backoff_multiplier})"
        )


# Initialize predefined policies after class definition
RetryPolicy.NONE = RetryPolicy(
    max_attempts=1, initial_delay_ms=0, max_delay_ms=0, backoff_multiplier=1.0
)

RetryPolicy.STANDARD = RetryPolicy(
    max_attempts=3,
    initial_delay_ms=1000,  # 1 second
    max_delay_ms=30000,  # 30 seconds
    backoff_multiplier=2.0,
)

RetryPolicy.AGGRESSIVE = RetryPolicy(
    max_attempts=10,
    initial_delay_ms=100,  # 100 milliseconds
    max_delay_ms=10000,  # 10 seconds
    backoff_multiplier=1.5,
)


# =============================================================================
# RetryableError - Fine-grained error retry control
# =============================================================================


class RetryableError(Exception):
    """
    Base class for errors that can specify whether they should be retried.

    Implement this to get fine-grained control over which errors are retried
    vs. cached as permanent failures.

    Example:
        class PaymentError(RetryableError):
            def __init__(self, message: str, is_retryable: bool = True):
                super().__init__(message)
                self._retryable = is_retryable

            def is_retryable(self) -> bool:
                return self._retryable

        # Transient error - should retry
        raise PaymentError("Network timeout", is_retryable=True)

        # Permanent error - should NOT retry
        raise PaymentError("Insufficient funds", is_retryable=False)

    From Rust:
        pub trait RetryableError {
            fn is_retryable(&self) -> bool;
        }
    """

    def is_retryable(self) -> bool:
        """
        Returns true if this error is transient and the operation should be retried.

        - True: Error is transient (network timeout, service unavailable).
          The step will NOT be cached, allowing retry on next execution.
        - False: Error is permanent (invalid input, business rule violation).
          The step WILL be cached, preventing retry.

        Returns:
            True if retryable, False if permanent
        """
        # Default: all errors are retryable (safe default)
        return True
