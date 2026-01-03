"""
SuspensionPayload - Data structure for suspension resolution.

**Rust Reference**:
`/home/richinex/Documents/devs/rust_projects/ergon/ergon/src/executor/child_flow.rs` lines 42-62

This module provides SuspensionPayload, which carries the result of a suspension
(timer or signal) back to the suspended flow.

**Python Documentation**:
- Dataclasses: https://docs.python.org/3/library/dataclasses.html
- bytes type: https://docs.python.org/3/library/stdtypes.html#bytes

From Dave Cheney: "Make the zero value useful"
All fields have sensible defaults where applicable.
"""

from dataclasses import dataclass

__all__ = ["SuspensionPayload"]


@dataclass(frozen=True)
class SuspensionPayload:
    """
    Payload structure for suspension mechanisms (signals, timers).

    **Rust Reference**: `src/executor/child_flow.rs` lines 42-62

    This is the data structure passed when resuming a suspended flow.
    It contains the result of what the flow was waiting for (child completion,
    timer expiry, or external signal).

    **Python Best Practice**: Using frozen dataclass for immutability
    Reference: BEST_PRACTICES.md section "Dataclasses (Data Structures)"

    Attributes:
        success: Whether the suspension resolved successfully (True) or failed (False)
        data: The serialized data:
              - For signals (child flows): the child flow's result or error message
              - For timers: empty bytes (timer just marks delay completion)
        is_retryable: Whether the error is retryable (only meaningful when success=False)
                     None means not applicable (success=True or non-retryable error)

    **From Rust**:
    ```rust
    pub struct SuspensionPayload {
        pub success: bool,
        pub data: Vec<u8>,
        #[serde(default)]
        pub is_retryable: Option<bool>,
    }
    ```

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

    Note:
        This is an immutable value type (frozen=True).
        Create new instances rather than modifying fields.
    """

    success: bool
    """Whether the suspension resolved successfully (True) or failed (False)"""

    data: bytes
    """
    Serialized data for the suspension result.

    - For child flows: pickled result or error
    - For timers: empty bytes b""
    - For external signals: application-specific data
    """

    is_retryable: bool | None = None
    """
    Whether the error is retryable (only meaningful when success=False).

    - None: Not applicable (success=True or non-retryable error)
    - True: Error can be retried
    - False: Error is permanent, don't retry
    """

    def __repr__(self) -> str:
        """Readable representation for debugging."""
        data_len = len(self.data)
        retryable_str = f", retryable={self.is_retryable}" if self.is_retryable is not None else ""
        return f"SuspensionPayload(success={self.success}, data_len={data_len}{retryable_str})"
