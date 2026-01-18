"""Protocol for type-safe child flow invocation with output type inference.

Extends FlowType with an Output type parameter, enabling static type
checkers to infer child flow result types at compile time.
"""

from typing import TYPE_CHECKING, Protocol, TypeVar, runtime_checkable

if TYPE_CHECKING:
    pass

__all__ = ["InvokableFlow"]

# Type variable for flow output type (can be any type)
Output = TypeVar("Output")


@runtime_checkable
class InvokableFlow(Protocol[Output]):
    """Protocol for flows that can be invoked as child flows with type safety.

    Extends FlowType with an Output type parameter for compile-time type
    inference when parent flows invoke child flows.
    """

    @staticmethod
    def type_id() -> str:
        """Return stable type identifier (inherited from FlowType)."""
        ...

    @staticmethod
    def output_type() -> type[Output]:
        """Return the output type of this flow for type inference.

        Must be implemented by child flow classes to enable type checking.
        """
        ...
