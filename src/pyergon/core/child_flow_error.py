"""Exception raised when child flow fails during parent execution.

Preserves error type, message, and retryability from the failed child flow.
"""

from pyergon.models import RetryableError

__all__ = ["ChildFlowError"]


class ChildFlowError(RetryableError):
    """Exception raised when a child flow fails during parent execution.

    Preserves error type, message, and retryability from the failed child,
    allowing parent flows to handle child failures with proper retry semantics.
    """

    def __init__(self, type_name: str, message: str, retryable: bool):
        super().__init__(message)
        self.type_name = type_name
        self.message = message
        self._retryable = retryable

    def is_retryable(self) -> bool:
        return self._retryable

    def __str__(self) -> str:
        return f"{self.type_name}: {self.message}"

    def __repr__(self) -> str:
        return (
            f"ChildFlowError(type_name={self.type_name!r}, "
            f"message={self.message!r}, retryable={self._retryable})"
        )
