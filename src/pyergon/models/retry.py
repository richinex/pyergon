"""Retry policy configuration for step execution.

Design: Strategy Pattern
    RetryPolicy encapsulates retry behavior, allowing different retry
    strategies without modifying step execution code.

Retry Philosophy:
    - Safe default: no automatic retries (backward compatible)
    - Simple retry: max_attempts=3 with standard exponential backoff
    - Advanced control: custom RetryPolicy for fine-grained tuning
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast


@dataclass(frozen=True)
class RetryPolicy:
    """Configuration for step retry behavior with exponential backoff.

    Controls maximum attempts and delay strategy for retrying failed steps.

    Usage:
        # Predefined policies with sensible defaults
        policy = RetryPolicy.STANDARD  # 3 attempts, 1s initial, 2x multiplier
        policy = RetryPolicy.AGGRESSIVE  # 10 attempts, 100ms initial, 1.5x multiplier

        # Simple: specify max attempts (uses standard delays)
        policy = RetryPolicy.with_max_attempts(5)

        # Custom: full control over backoff strategy
        policy = RetryPolicy(
            max_attempts=5,
            initial_delay_ms=1000,
            max_delay_ms=30000,
            backoff_multiplier=2.0
        )

    Immutable: frozen=True prevents modification after creation.
    """

    max_attempts: int
    """Maximum number of attempts including the first try.

    Examples:
        - max_attempts=1: no retries (one attempt only)
        - max_attempts=3: initial attempt + 2 retries
    """

    initial_delay_ms: int
    """Initial delay before first retry in milliseconds (e.g., 1000 = 1 second)."""

    max_delay_ms: int
    """Maximum delay between retries in milliseconds (caps exponential backoff)."""

    backoff_multiplier: float
    """Multiplier for exponential backoff (e.g., 2.0 doubles delay each retry).

    Delay formula: min(initial_delay * multiplier^(attempt-1), max_delay)
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
        """Create policy with custom max_attempts using standard delays.

        Args:
            max_attempts: Maximum number of attempts

        Returns:
            RetryPolicy with standard delays (1s initial, 30s max, 2x multiplier)

        Example:
            ```python
            policy = RetryPolicy.with_max_attempts(5)
            ```
        """
        return cls(
            max_attempts=max_attempts,
            initial_delay_ms=1000,
            max_delay_ms=30000,
            backoff_multiplier=2.0,
        )

    def delay_for_attempt(self, attempt: int) -> int | None:
        """Calculate delay before next retry using exponential backoff.

        Args:
            attempt: Current attempt number (1-indexed)

        Returns:
            Delay in milliseconds, or None if max attempts exceeded

        Example:
            ```python
            policy = RetryPolicy.STANDARD
            policy.delay_for_attempt(1)  # 1000ms
            policy.delay_for_attempt(2)  # 2000ms
            policy.delay_for_attempt(3)  # None (max reached)
            ```
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
    """Base exception for errors with fine-grained retry control.

    Subclass this to specify whether specific error instances should
    be retried or cached as permanent failures.

    Example:
        ```python
        class PaymentError(RetryableError):
            def __init__(self, message: str, retryable: bool = True):
                super().__init__(message)
                self._retryable = retryable

            def is_retryable(self) -> bool:
                return self._retryable

        # Transient error - will retry
        raise PaymentError("Network timeout", retryable=True)

        # Permanent error - will cache and not retry
        raise PaymentError("Insufficient funds", retryable=False)
        ```
    """

    def is_retryable(self) -> bool:
        """Return whether this error is transient and should be retried.

        Returns:
            True if error is transient (network timeout, service unavailable).
                Step will NOT be cached, allowing retry.
            False if error is permanent (invalid input, business rule violation).
                Step WILL be cached, preventing further retries.

        Default: True (all errors retryable for safety)
        """
        return True
