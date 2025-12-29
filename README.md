# Ergon - Durable Execution Framework for Python

**Pure Python** implementation of durable execution with Temporal-like semantics.

## Features

- ✅ **Durable Steps**: Automatically cached and retried on failure
- ✅ **Durable Timers**: Timers survive process restarts
- ✅ **Distributed Workers**: Multiple workers process flows in parallel
- ✅ **Work Stealing**: Fair load distribution across workers
- ✅ **Storage Backends**: SQLite and in-memory implementations
- ✅ **Pure Python**: No Rust/PyO3 dependencies, just aiosqlite

## Design Principles

### From Dave Cheney's Practical Go (adapted for Python)
- Simple, readable code over clever abstractions
- Clear naming (no cryptic abbreviations)
- Explicit dependencies (no globals)
- Errors handled once (no log AND raise)
- Guard clauses (return early)

### From Software Design Patterns (Python)
- **Template Method**: Worker main loop structure
- **Strategy**: Pluggable flow handlers
- **Adapter**: Unified storage interface
- **Observer**: Timer notification system
- **Façade**: Simplified scheduling API
- **Builder**: Fluent worker configuration

### SOLID Principles
- **Single Responsibility**: Each component has one purpose
- **Open-Closed**: Extend via protocols, not modification
- **Liskov Substitution**: Storage backends interchangeable
- **Interface Segregation**: Focused protocols
- **Dependency Inversion**: Depend on abstractions

## Installation

```bash
# Clone repository
git clone <repo-url>
cd llm_graph_python

# Install in development mode
pip install -e .
```

**Dependencies:**
- Python 3.11+
- aiosqlite >= 0.19.0

## Quick Start

```python
import asyncio
from ergon import flow, step, SqliteExecutionLog

@flow
class OrderProcessor:
    def __init__(self, order_id: str, amount: float):
        self.order_id = order_id
        self.amount = amount

    @step
    async def validate(self):
        print(f"[{self.order_id}] Validating...")
        return self.amount > 0

    @step
    async def process_payment(self):
        print(f"[{self.order_id}] Processing ${self.amount}...")
        return f"payment-{self.order_id}"

    async def run(self):
        await self.validate()
        return await self.process_payment()

async def main():
    # Setup storage
    storage = SqliteExecutionLog("orders.db")
    await storage.connect()

    # Execute workflow
    order = OrderProcessor("ORD-001", 100.0)
    result = await order.run()

    print(f"Result: {result}")

    await storage.close()

asyncio.run(main())
```

## Distributed Workers with Timers

```python
from ergon import (
    flow, step,
    SqliteExecutionLog,
    FlowScheduler,
    FlowWorker,
    schedule_timer_named
)

@flow
class TimedOrderProcessor:
    def __init__(self, order_id: str):
        self.order_id = order_id

    @step
    async def wait_for_fraud_check(self):
        print(f"[{self.order_id}] Waiting for fraud check...")
        await schedule_timer_named(2.0, f"fraud-{self.order_id}")
        print(f"[{self.order_id}] Fraud check complete")

    async def process(self):
        await self.wait_for_fraud_check()
        return "completed"

async def main():
    # Setup
    storage = SqliteExecutionLog("distributed.db")
    await storage.connect()

    scheduler = FlowScheduler(storage)

    # Start workers with timer processing
    worker1 = FlowWorker(storage, "worker-1") \\
        .with_timers(0.1) \\
        .with_poll_interval(0.1)

    await worker1.register(lambda flow: flow.process())
    handle1 = await worker1.start()

    worker2 = FlowWorker(storage, "worker-2") \\
        .with_timers(0.1) \\
        .with_poll_interval(0.1)

    await worker2.register(lambda flow: flow.process())
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

## Project Structure

```
llm_graph_python/
├── src/ergon/              # Pure Python implementation
│   ├── core/              # Core types (Invocation, Status)
│   ├── storage/           # Storage backends (SQLite, Memory)
│   ├── executor/          # Execution engine (Scheduler, Worker, Timer)
│   ├── decorators.py      # @flow and @step decorators
│   ├── context.py         # Execution context
│   └── __init__.py        # Public API
│
├── examples/               # Example workflows
│   └── distributed_worker_timer_sqlite.py
│
├── docs/                   # Documentation
│   ├── ARCHITECTURE.md     # Design document
│   ├── IMPLEMENTATION_SUMMARY.md  # Implementation details
│   └── CLEANUP_CHECKLIST.md       # Migration guide
│
├── tests/                  # Test suite (TODO)
└── pyproject.toml          # Pure Python configuration
```

## Architecture

### Core Components

1. **Core Types** (`ergon.core`)
   - `Invocation`: Single step execution record
   - `InvocationStatus`: Step state machine
   - `ScheduledFlow`: Distributed queue item

2. **Storage Layer** (`ergon.storage`)
   - `ExecutionLog`: Abstract protocol (Adapter pattern)
   - `SqliteExecutionLog`: SQLite backend
   - `InMemoryExecutionLog`: In-memory backend (testing)

3. **Executor** (`ergon.executor`)
   - `FlowScheduler`: Enqueue flows (Single Responsibility)
   - `FlowWorker`: Process flows (Template Method)
   - `schedule_timer`: Durable timers (Observer pattern)

4. **Decorators** (`ergon.decorators`)
   - `@flow`: Mark workflow class
   - `@step`: Mark durable step method

### Design Patterns

| Pattern | Usage | Module |
|---------|-------|--------|
| Template Method | Worker main loop | `FlowWorker._run()` |
| Strategy | Flow handlers | `FlowWorker.register()` |
| Adapter | Storage backends | `SqliteExecutionLog` |
| Observer | Timer notifications | `schedule_timer()` |
| Façade | Simplified scheduling | `FlowScheduler` |
| Builder | Fluent configuration | `worker.with_timers()` |
| Protocol | Structural typing | `ExecutionLog` |

## Examples

See `examples/distributed_worker_timer_sqlite.py` for a complete example demonstrating:
- Multiple workers processing flows
- Durable timer coordination
- Worker distribution tracking
- Graceful shutdown

```bash
python examples/distributed_worker_timer_sqlite.py
```

## Testing

```bash
# Run tests (TODO: implement)
pytest tests/

# Type checking
mypy src/ergon/

# Linting
ruff check src/ergon/
```

## Development

### Design Principles Applied

Every component follows:
1. **Dave Cheney's Practical Go** principles (adapted for Python)
2. **Software Design Patterns** from the Python reference
3. **SOLID** principles for maintainability

### Code Quality

- Comprehensive docstrings with pattern citations
- Type hints throughout
- Dave Cheney principle references
- Design pattern explanations
- Example usage in every module

## Documentation

- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Complete design document
- **[IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md)** - Implementation details
- **[CLEANUP_CHECKLIST.md](CLEANUP_CHECKLIST.md)** - Migration from PyO3

## Statistics

- **~2,000 lines** of pure Python
- **8 design patterns** explicitly applied
- **5 SOLID principles** followed
- **0 Rust dependencies** (pure Python!)

## License

MIT / Apache 2.0 (dual license)

## Credits

Inspired by:
- [Temporal](https://temporal.io/) - Durable execution engine
- [Dave Cheney](https://dave.cheney.net/) - Practical Go principles
- Software Design Patterns in Python

---

**Pure Python. No Magic. Clear Principles.**
