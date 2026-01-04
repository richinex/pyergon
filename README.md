# PyErgon - Durable Execution Framework for Python

Pure Python implementation of durable execution with Temporal-like semantics.

## Features

- **Durable Steps**: Automatically cached and retried on failure
- **Durable Timers**: Event-driven timers survive process restarts
- **Distributed Workers**: Multiple workers process flows from shared queue
- **Event-Driven Architecture**: Workers wake on events (new work, timer expiry, new timer)
- **Storage Backends**: SQLite, Redis, and in-memory implementations

## Design Philosophy

PyErgon follows practical software engineering principles:

- Simple, readable code over clever abstractions
- Clear naming without cryptic abbreviations
- Explicit dependencies and no global state
- Errors handled once at the appropriate level
- Type hints and protocols for structural typing
- Composable components with focused responsibilities

## Installation

```bash
# Clone repository
git clone <repo-url>
cd pyergon

# Install dependencies with uv
uv sync
```

**Dependencies:**
- Python 3.11+
- aiosqlite >= 0.19.0

## Quick Start

```python
import asyncio
from dataclasses import dataclass
from pyergon import flow, flow_type, step, Executor
from pyergon.storage.sqlite import SqliteExecutionLog

@dataclass
@flow_type
class OrderProcessor:
    order_id: str
    amount: float

    @step
    async def validate(self):
        print(f"[{self.order_id}] Validating...")
        return self.amount > 0

    @step
    async def process_payment(self):
        print(f"[{self.order_id}] Processing ${self.amount}...")
        return f"payment-{self.order_id}"

    @flow
    async def run(self):
        await self.validate()
        return await self.process_payment()

async def main():
    # Setup storage
    storage = await SqliteExecutionLog.in_memory()

    # Execute workflow
    order = OrderProcessor("ORD-001", 100.0)
    executor = Executor(order, storage, "order-001")
    outcome = await executor.run(lambda o: o.run())

    print(f"Result: {outcome.result}")

    await storage.close()

asyncio.run(main())
```

## Distributed Workers with Timers

```python
from dataclasses import dataclass
from pyergon import flow, flow_type, step, Scheduler, Worker
from pyergon.storage.sqlite import SqliteExecutionLog
from pyergon.executor.timer import schedule_timer_named
from pyergon.core import TaskStatus

@dataclass
@flow_type
class TimedOrderProcessor:
    order_id: str

    @step
    async def wait_for_fraud_check(self):
        print(f"[{self.order_id}] Waiting for fraud check...")
        await schedule_timer_named(2.0, f"fraud-{self.order_id}")
        print(f"[{self.order_id}] Fraud check complete")

    @flow
    async def process(self):
        await self.wait_for_fraud_check()
        return "completed"

async def main():
    # Setup
    storage = SqliteExecutionLog("distributed.db")
    await storage.connect()

    scheduler = Scheduler(storage)

    # Start workers with timer processing
    worker1 = Worker(storage, "worker-1", enable_timers=True)
    await worker1.register(TimedOrderProcessor)
    handle1 = await worker1.start()

    worker2 = Worker(storage, "worker-2", enable_timers=True)
    await worker2.register(TimedOrderProcessor)
    handle2 = await worker2.start()

    # Schedule flows
    for i in range(3):
        order = TimedOrderProcessor(f"ORD-{i:03d}")
        await scheduler.schedule(order)

    # Wait for completion
    await asyncio.sleep(5)

    # Shutdown
    await handle1.shutdown()
    await handle2.shutdown()
    await storage.close()

asyncio.run(main())
```

## Architecture

### Core Components

1. **Core Types** (`pyergon.core`)
   - `Invocation`: Single step execution record
   - `InvocationStatus`: Step state machine
   - `ScheduledFlow`: Distributed queue item

2. **Storage Layer** (`pyergon.storage`)
   - `ExecutionLog`: Abstract protocol for persistence
   - `SqliteExecutionLog`: SQLite backend
   - `InMemoryExecutionLog`: In-memory backend for testing

3. **Executor** (`pyergon.executor`)
   - `Executor`: Execute flows with durable context
   - `Scheduler`: Enqueue flows for distributed processing
   - `Worker`: Process flows from queue
   - `schedule_timer`: Durable timers
   - `await_external_signal`: External event coordination

4. **Decorators** (`pyergon.decorators`)
   - `@flow_type`: Mark workflow class
   - `@flow`: Mark flow entry point method
   - `@step`: Mark durable step method

## Examples

See `examples/` directory for complete examples:

```bash
PYTHONPATH=src uv run python examples/simple_timer_sqlite.py
```

## Testing

```bash
# Run all tests (60 tests, 48% coverage)
uv run pytest tests/

# Run specific test file
uv run pytest tests/test_durability.py -v

# Type checking
mypy src/pyergon/

# Linting
ruff check src/pyergon/
```

## Development

### Code Quality

- Comprehensive docstrings
- Type hints throughout
- Protocol-based interfaces
- Example usage in every module
- Test coverage with property-based testing


## License

MIT / Apache 2.0 (dual license)

## Credits

Inspired by:
- [Temporal](https://temporal.io/) - Durable execution engine
- [Dave Cheney](https://dave.cheney.net/) - Practical Go principles
