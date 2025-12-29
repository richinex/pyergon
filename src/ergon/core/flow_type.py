"""
FlowType protocol for stable type identification.

This module provides FlowType protocol which ensures flows have stable
type identifiers that don't change across Python versions or refactorings.

RUST COMPLIANCE: Matches Rust ergon src/core/flow_type.rs

From Rust:
    pub trait FlowType {
        fn type_id() -> &'static str;
    }

Design: Protocol-based (PEP 544) for structural typing
No inheritance required - any class with type_id() method qualifies.

From Dave Cheney:
"Let functions define the behavior they require" - Worker only needs
type_id() method, not full flow interface. Protocol enables this.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class FlowType(Protocol):
    """
    Protocol for flows with stable type identification.

    FlowType ensures flows can be identified by a stable string ID
    that doesn't change across Python versions, unlike __qualname__ or
    __module__ which can vary.

    This is CRITICAL for distributed workers:
    - Worker registers handler for "MyFlow" type ID
    - Scheduler serializes flow with "MyFlow" type ID
    - Worker deserializes and routes to correct handler

    From Rust ergon src/core/flow_type.rs:
        pub trait FlowType {
            fn type_id() -> &'static str;
        }

    Why NOT use type(flow).__name__:
        1. Can change if class is renamed or moved to another module
        2. Not stable across different Python versions
        3. Generic types have complex names (Flow[T] not Flow)

    Why use explicit type_id():
        1. Stable - developer controls the ID
        2. Clear - not tied to Python internals
        3. Flexible - can have custom IDs (#[flow_type(id = "custom")])

    Usage:
        @flow
        class MyFlow:
            @staticmethod
            def type_id() -> str:
                return "MyFlow"

        # Check if class implements protocol
        assert isinstance(MyFlow, FlowType)

        # Get type ID
        flow_type_id = MyFlow.type_id()  # "MyFlow"
    """

    @staticmethod
    def type_id() -> str:
        """
        Return stable type identifier for this flow.

        This MUST return a consistent string across:
        - Python versions (3.11, 3.12, etc.)
        - Refactorings (moving classes, renaming)
        - Serialization round-trips

        Default: Class name (e.g., "MyFlow")
        Custom: Specified via decorator parameter

        Returns:
            Stable type identifier string

        Example:
            class OrderProcessor:
                @staticmethod
                def type_id() -> str:
                    return "OrderProcessor"
        """
        ...


def get_flow_type_id(flow: FlowType) -> str:
    """
    Get type ID from a flow instance or class.

    From Rust:
        T::type_id()

    Args:
        flow: Flow instance or class implementing FlowType

    Returns:
        Type identifier string

    Raises:
        TypeError: If flow doesn't implement FlowType protocol

    Usage:
        flow = MyFlow()
        type_id = get_flow_type_id(flow)  # "MyFlow"

        # Or with class
        type_id = get_flow_type_id(MyFlow)  # "MyFlow"
    """
    if isinstance(flow, type):
        # flow is a class
        return flow.type_id()
    else:
        # flow is an instance
        return flow.__class__.type_id()
