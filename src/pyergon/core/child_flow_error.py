"""
ChildFlowError - Exception raised when child flow fails.

**Rust Reference**: ergon_rust/ergon/src/executor/error.rs ExecutionError::User

This module provides ChildFlowError, which preserves error information
from failed child flows including the retryability flag.

**Python Documentation**:
- Exceptions: https://docs.python.org/3/library/exceptions.html
"""

from pyergon.models import RetryableError

__all__ = ["ChildFlowError"]


class ChildFlowError(RetryableError):
    """
    Exception raised when a child flow fails.

    **Rust Reference**: ExecutionError::User variant

    ```rust
    pub enum ExecutionError {
        User {
            type_name: String,
            message: String,
            retryable: bool,
        },
        // ... other variants
    }
    ```

    This exception preserves the error type name, message, and retryability
    flag from the child flow's failure. The retryability flag determines
    whether the parent flow should retry when this error occurs.

    Attributes:
        type_name: The error type name from the child flow
        message: The error message from the child flow
        retryable: Whether this error is retryable
    """

    def __init__(self, type_name: str, message: str, retryable: bool):
        """
        Create a ChildFlowError.

        Args:
            type_name: The error type name from the child flow
            message: The error message from the child flow
            retryable: Whether this error is retryable
        """
        super().__init__(message)
        self.type_name = type_name
        self.message = message
        self._retryable = retryable

    def is_retryable(self) -> bool:
        """
        Check if this error is retryable.

        Returns:
            True if the error can be retried, False otherwise
        """
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
