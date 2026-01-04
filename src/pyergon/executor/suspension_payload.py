"""Suspension resolution payload data structure.

Provides SuspensionPayload, which carries the result of a suspension
(timer or signal) back to the suspended flow when it resumes.
"""

from dataclasses import dataclass

__all__ = ["SuspensionPayload"]


@dataclass(frozen=True)
class SuspensionPayload:
    """Payload structure for suspension resolution.

    Data structure passed when resuming a suspended flow. Contains
    the result of what the flow was waiting for (child completion,
    timer expiry, or external signal).

    Attributes:
        success: Whether suspension resolved successfully or failed
        data: Serialized data (child flow result, error message, or empty for timers)
        is_retryable: Whether error is retryable (only meaningful when success=False)

    Example:
        ```python
        # Successful child flow completion
        payload = SuspensionPayload(
            success=True,
            data=pickle.dumps({"order_id": "123"}),
            is_retryable=None
        )

        # Failed with retryable error
        payload = SuspensionPayload(
            success=False,
            data=pickle.dumps("Connection timeout"),
            is_retryable=True
        )

        # Timer expiry (no data)
        payload = SuspensionPayload(
            success=True,
            data=b"",
            is_retryable=None
        )
        ```
    """

    success: bool
    data: bytes
    is_retryable: bool | None = None

    def __repr__(self) -> str:
        """Readable representation for debugging."""
        data_len = len(self.data)
        retryable_str = f", retryable={self.is_retryable}" if self.is_retryable is not None else ""
        return f"SuspensionPayload(success={self.success}, data_len={data_len}{retryable_str})"
