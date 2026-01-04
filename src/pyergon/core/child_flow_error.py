"""Exception raised when child flow fails during parent execution.

Preserves error type, message, and retryability from the failed child flow.
"""

from pyergon.models import RetryableError

__all__ = ["ChildFlowError"]


class ChildFlowError(RetryableError):
    """Exception raised when a child flow fails during parent execution.

    Preserves error type, message, and retryability from the failed child,
    allowing parent flows to handle child failures with proper retry semantics.

    The retryability flag determines whether the parent flow should retry
    when this error occurs.

    Example:
        ```python
        # Child flow fails with permanent error
        raise ChildFlowError(
            type_name="ValidationError",
            message="Invalid product ID",
            retryable=False
        )
        ```

    Attributes:
        type_name: Error type name from the child flow
        message: Error message from the child flow
        _retryable: Whether this error should trigger retries
    """

    def __init__(self, type_name: str, message: str, retryable: bool):
        """Create a ChildFlowError with child's error information.

        Args:
            type_name: Error type name from child flow
            message: Error message from child flow
            retryable: Whether parent should retry on this error
        """
        super().__init__(message)
        self.type_name = type_name
        self.message = message
        self._retryable = retryable

    def is_retryable(self) -> bool:
        """Return whether parent flow should retry on this child error."""
        return self._retryable

    def __str__(self) -> str:
        """String representation of the error."""
        return f"{self.type_name}: {self.message}"

    def __repr__(self) -> str:
        """Readable representation for debugging."""
        return (
            f"ChildFlowError(type_name={self.type_name!r}, "
            f"message={self.message!r}, retryable={self._retryable})"
        )
