"""
InvokableFlow protocol for child flow invocation.

**Rust Reference**:
`/home/richinex/Documents/devs/rust_projects/ergon/ergon/src/core/flow_type.rs`
lines 103-137

This module provides the InvokableFlow protocol which extends FlowType
with an Output type for type-safe parent-child invocation.

**Python Documentation**:
- Protocol: https://docs.python.org/3/library/typing.html#typing.Protocol
- Generic: https://docs.python.org/3/library/typing.html#typing.Generic
- PEP 544: https://peps.python.org/pep-0544/

From Dave Cheney: "Define errors out of existence"
By using compile-time type checking, we eliminate runtime type errors
when invoking child flows.
"""

from typing import TYPE_CHECKING, Protocol, TypeVar, runtime_checkable

if TYPE_CHECKING:
    pass

__all__ = ["InvokableFlow"]

# Type variable for flow output type
# Bound to object to allow any type
# Reference: https://docs.python.org/3/library/typing.html#typing.TypeVar
Output = TypeVar("Output")


@runtime_checkable
class InvokableFlow(Protocol[Output]):
    """
    Protocol for flows that can be invoked as child flows.

    **Rust Reference**: `src/core/flow_type.rs` lines 131-137

    This protocol extends FlowType with an associated Output type,
    enabling compile-time type inference when invoking child flows.

    **Python Best Practice**: Using Protocol for structural subtyping
    Reference: BEST_PRACTICES.md section "Protocols (Structural Subtyping)"

    **Type Safety**:
    The Output type parameter enables the compiler to infer the child's
    result type automatically:

    ```python
    @flow
    class CheckInventory(InvokableFlow[InventoryStatus]):
        product_id: str

        @staticmethod
        def output_type() -> type[InventoryStatus]:
            return InventoryStatus

        @flow
        async def run(self) -> InventoryStatus:
            # Implementation
            pass

    # Parent can invoke with type inference:
    @flow
    class OrderFlow:
        @flow
        async def process(self):
            # Type checker knows inventory is InventoryStatus
            inventory = await self.invoke(
                CheckInventory(product_id="PROD-123")
            ).result()
    ```

    **Backwards Compatibility**:
    This protocol is separate from FlowType to maintain backwards
    compatibility. Existing flows that don't use child invocation
    don't need to implement this protocol.

    Example:
        ```python
        from dataclasses import dataclass
        from pyergon import flow, InvokableFlow

        @dataclass
        class ShippingLabel:
            tracking: str

        @flow
        class LabelFlow(InvokableFlow[ShippingLabel]):
            parent_id: int

            @staticmethod
            def output_type() -> type[ShippingLabel]:
                return ShippingLabel

            @flow
            async def generate(self) -> ShippingLabel:
                return ShippingLabel(
                    tracking=f"TRK-{self.parent_id}"
                )

        # Parent flow invokes child
        @flow
        class OrderFlow:
            @flow
            async def run(self):
                label = await self.invoke(
                    LabelFlow(parent_id=123)
                ).result()
                # label is type ShippingLabel ✓
        ```
    """

    @staticmethod
    def type_id() -> str:
        """
        Return stable type identifier.

        Inherited from FlowType protocol.

        **Rust Reference**: `FlowType::type_id()`

        Returns:
            Stable string identifier for this flow type

        Note:
            This is automatically provided by the @flow decorator.
        """
        ...

    @staticmethod
    def output_type() -> type[Output]:
        """
        Return the output type of this flow.

        **Python-Specific Method**:
        In Rust, this is an associated type `type Output = T`.
        In Python, we use a method that returns the type object.

        **Type Checking**: Static type checkers can use this for inference
        Reference: https://docs.python.org/3/library/typing.html#typing.get_type_hints

        Returns:
            Type object for this flow's output

        Example:
            ```python
            @flow
            class MyFlow(InvokableFlow[MyResult]):
                @staticmethod
                def output_type() -> type[MyResult]:
                    return MyResult
            ```

        Note:
            Must be implemented by child flow classes.
            The @flow decorator does NOT generate this automatically.
        """
        ...
