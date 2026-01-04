"""Protocol for stable flow type identification across serialization.

Provides FlowType protocol ensuring flows have stable type identifiers
that survive Python version changes, refactorings, and serialization.

Design: Structural Typing (PEP 544)
    No inheritance required - any class with type_id() method qualifies.
    Workers only need type_id() for routing, not the full flow interface.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class FlowType(Protocol):
    """Protocol for flows with stable, version-independent type identification.

    Provides explicit type IDs that remain stable across Python versions,
    refactorings, and module reorganizations.

    Critical for distributed execution:
        - Worker registers handler for "OrderProcessor" type ID
        - Scheduler serializes flow with "OrderProcessor" type ID
        - Worker deserializes and routes to correct handler

    Why NOT use type(flow).__name__:
        - Changes if class is renamed or moved to another module
        - Not stable across different Python versions
        - Generic types have complex names (MyGeneric[T] not MyGeneric)

    Why use explicit type_id():
        - Stable: developer controls the ID
        - Clear: not tied to Python internals
        - Flexible: can customize via decorator parameter

    Example:
        ```python
        from pyergon.decorators import flow_type

        @flow_type
        class OrderProcessor:
            @staticmethod
            def type_id() -> str:
                return "OrderProcessor"

        # Check protocol implementation
        assert isinstance(OrderProcessor, FlowType)

        # Get type ID
        type_id = OrderProcessor.type_id()  # "OrderProcessor"
        ```
    """

    @staticmethod
    def type_id() -> str:
        """Return stable type identifier for this flow.

        Must return a consistent string across:
            - Python versions (3.11, 3.12, etc.)
            - Refactorings (moving classes, renaming modules)
            - Serialization round-trips

        Returns:
            Stable type identifier (typically class name)

        Example:
            ```python
            class OrderProcessor:
                @staticmethod
                def type_id() -> str:
                    return "OrderProcessor"
            ```
        """
        ...


def get_flow_type_id(flow: FlowType) -> str:
    """Extract type ID from a flow instance or class.

    Args:
        flow: Flow instance or class implementing FlowType

    Returns:
        Type identifier string

    Raises:
        TypeError: If flow doesn't implement FlowType protocol

    Example:
        ```python
        # From instance
        flow = OrderProcessor()
        type_id = get_flow_type_id(flow)  # "OrderProcessor"

        # From class
        type_id = get_flow_type_id(OrderProcessor)  # "OrderProcessor"
        ```
    """
    if isinstance(flow, type):
        # flow is a class
        return flow.type_id()
    else:
        # flow is an instance
        return flow.__class__.type_id()
