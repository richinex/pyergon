# PyErgon

[![PyPI version](https://badge.fury.io/py/pyergon.svg)](https://pypi.org/project/pyergon/)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

A minimal durable execution framework for Python, based on SQLite and Redis.

PyErgon stores method invocations in a database. When restarting a flow after a failure (e.g., machine crash), it resumes from the last successful step, driving it to completion. Flows can suspend awaiting external signals (human approval, webhook callbacks) or timers, and automatically resume when ready.

**Website**: https://github.com/richinex/pyergon
**Documentation**: https://github.com/richinex/pyergon
**Source code**: https://github.com/richinex/pyergon
**Bug reports**: https://github.com/richinex/pyergon/issues

## What is PyErgon?

PyErgon is a durable execution framework that:

- **Survives crashes**: Flows resume from last successful step after machine/process failures
- **Suspends awaiting signals**: Flows pause for external events (approvals, webhooks) and automatically resume
- **Caches expensive steps**: Completed steps never re-execute on retry
- **Scales horizontally**: Multiple workers process flows from shared queue (SQLite or Redis)
- **Supports complex orchestration**: Child flows, parallel DAG execution, timers, retry policies

It provides:

- `@flow` and `@step` decorators for durable workflows
- Storage backends (SQLite, Redis, in-memory)
- Automatic retry with exponential backoff
- External signal coordination
- Event-driven worker architecture
- Type-safe APIs with protocols

## Installation

### From PyPI (Recommended)

```bash
pip install pyergon
```

Requires Python 3.11 or higher.

### From Source

```bash
# Clone repository
git clone https://github.com/richinex/pyergon.git
cd pyergon

# Install with uv
uv sync

# Or with pip
pip install -e .
```

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

## Advanced Features

### Waiting for External Signals ("Human in the Loop")

Flows can suspend awaiting external signals (approvals, webhooks, callbacks):

```python
from pyergon.executor.signal import await_external_signal

@dataclass
@flow_type
class DocumentApprovalFlow:
    document_id: str

    @flow
    async def process(self):
        await self.validate_document()

        # Suspend until manager approves
        decision = await self.await_manager_approval()

        if not decision.approved:
            raise ManagerRejectionError(decision.reason)

        await self.publish_document()
        return "approved"

    @step
    async def await_manager_approval(self):
        signal_name = f"manager_approval_{self.document_id}"
        decision_bytes = await await_external_signal(signal_name)
        return pickle.loads(decision_bytes)
```

Resume the flow externally:

```python
worker = Worker(storage, "worker-1")
worker = worker.with_signals(signal_source, poll_interval=0.5)
await worker.register(DocumentApprovalFlow)
await worker.start()

# Later, from webhook or API:
await signal_source.send_signal("manager_approval_DOC-001", decision_data)
```

### Child Flow Invocation

Parent flows can invoke child flows and await their results:

```python
@dataclass
@flow_type(invokable=str)
class PaymentFlow:
    order_id: str
    amount: float

    @flow(retry_policy=RetryPolicy.STANDARD)
    async def process(self):
        await self.charge_card()
        return f"PAYMENT-{self.order_id}"

@dataclass
@flow_type
class OrderFlow:
    order_id: str
    amount: float

    @flow
    async def process(self):
        await self.validate_order()

        # Invoke child flow and await result
        payment_flow = PaymentFlow(self.order_id, self.amount)
        payment_id = await self.invoke(payment_flow).result()

        return f"Order complete: {payment_id}"
```

### Retry Policies

Control retry behavior for transient vs permanent errors:

```python
class ApiTimeoutError(RetryableError):
    def is_retryable(self) -> bool:
        return True  # Will be retried

class ItemNotFoundError(RetryableError):
    def is_retryable(self) -> bool:
        return False  # Fails immediately

@flow(retry_policy=RetryPolicy.STANDARD)
async def process_order(self):
    await self.check_inventory()  # Retries on ApiTimeoutError
    await self.process_payment()   # Fails fast on ItemNotFoundError
```

## Examples

Complete examples in `examples/` directory:

| Example | Demonstrates |
|---------|-------------|
| `comprehensive_demo.py` | Full orchestration with versioning, child flows, DAG execution |
| `external_signal_abstraction.py` | Document approval workflow with external signals |
| `nested_flows.py` | Parent-child flow execution patterns |
| `retryable_error_proof.py` | Sophisticated retry behavior |
| `numpy_computation_pipeline.py` | Scientific computing pipeline with step caching |
| `concurrent_segmented_sieve.py` | Performance benchmarks with parallel workers |

Run examples:

```bash
# From PyPI installation
python examples/comprehensive_demo.py

# From source
PYTHONPATH=src python examples/comprehensive_demo.py
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
