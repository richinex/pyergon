"""Protocol for type-safe child flow invocation with output type inference.

Extends FlowType with an Output type parameter, enabling static type
checkers to infer child flow result types at compile time.

Design: Eliminate Runtime Type Errors
    By using Protocol[Output], type checkers catch mismatches before
    code runs, eliminating a whole class of runtime errors.
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

    Type Safety Benefits:
        Static type checkers automatically infer child result types,
        catching type mismatches before runtime.

    Backwards Compatibility:
        Separate from FlowType to avoid breaking existing flows.
        Only implement this when creating child flows that return results.

    Example:
        ```python
        from dataclasses import dataclass
        from pyergon import flow_type, flow, InvokableFlow

        @dataclass
        class InventoryStatus:
            available: int

        @flow_type(invokable=InventoryStatus)
        class CheckInventory(InvokableFlow[InventoryStatus]):
            product_id: str

            @staticmethod
            def output_type() -> type[InventoryStatus]:
                return InventoryStatus

            @flow
            async def run(self) -> InventoryStatus:
                return InventoryStatus(available=42)

        # Parent flow with type inference
        @flow_type
        class OrderFlow:
            @flow
            async def process(self):
                # Type checker knows result is InventoryStatus
                result = await self.invoke(
                    CheckInventory(product_id="PROD-123")
                ).result()
                print(f"Available: {result.available}")
        ```
    """

    @staticmethod
    def type_id() -> str:
        """Return stable type identifier (inherited from FlowType).

        Returns:
            Stable string identifier for this flow type

        Note:
            Automatically provided by @flow_type decorator.
        """
        ...

    @staticmethod
    def output_type() -> type[Output]:
        """Return the output type of this flow for type inference.

        Must be implemented by child flow classes to enable type checking.

        Returns:
            Type object for this flow's output

        Example:
            ```python
            @dataclass
            class OrderResult:
                order_id: str

            @flow_type(invokable=OrderResult)
            class ProcessOrder(InvokableFlow[OrderResult]):
                @staticmethod
                def output_type() -> type[OrderResult]:
                    return OrderResult
            ```

        Note:
            The @flow_type decorator does NOT generate this automatically.
            You must implement it explicitly.
        """
        ...
