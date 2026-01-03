"""
TimerInfo - Information about an expired timer.

**Rust Reference**: ergon_rust/ergon/src/storage/mod.rs lines 66-71

This module provides TimerInfo, which represents metadata about an expired timer
returned from storage.

**Python Documentation**:
- Dataclasses: https://docs.python.org/3/library/dataclasses.html
"""

from dataclasses import dataclass
from datetime import datetime

__all__ = ["TimerInfo"]


@dataclass(frozen=True)
class TimerInfo:
    """
    Information about an expired timer.

    **Rust Reference**: `src/storage/mod.rs` lines 66-71

    ```rust
    pub struct TimerInfo {
        pub flow_id: Uuid,
        pub step: i32,
        pub fire_at: DateTime<Utc>,
        pub timer_name: Option<String>,
    }
    ```

    Attributes:
        flow_id: Flow identifier waiting for timer
        step: Step number waiting for timer
        fire_at: When the timer is scheduled to fire
        timer_name: Optional timer name for debugging
    """

    flow_id: str
    """Flow identifier waiting for timer"""

    step: int
    """Step number waiting for timer"""

    fire_at: datetime
    """When the timer is scheduled to fire"""

    timer_name: str | None = None
    """Optional timer name for debugging"""

    def __repr__(self) -> str:
        """Readable representation for debugging."""
        name_str = f", name={self.timer_name!r}" if self.timer_name else ""
        return (
            f"TimerInfo(flow_id={self.flow_id!r}, step={self.step}, "
            f"fire_at={self.fire_at.isoformat()}{name_str})"
        )
