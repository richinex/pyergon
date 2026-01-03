"""
External Signal Abstraction Example - Custom Error Variant

This example demonstrates using **custom domain errors** for error handling.

## Pattern Shown: Custom Errors

This variant uses custom domain error types in flows and steps:
- Simpler than framework errors
- Direct domain error types in signatures
- Framework errors converted at boundaries

## Key Features

- Signal source abstraction via SignalSource protocol
- Worker integration for automatic signal processing
- Modeling signal outcomes (approved/rejected) vs errors

## Run with:
```bash
PYTHONPATH=src python examples/external_signal_abstraction.py
```
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Dict, Protocol
import pickle

from pyergon import flow, flow_type, step, Scheduler, Worker
from pyergon.storage.sqlite import SqliteExecutionLog
from pyergon.executor.signal import await_external_signal
from pyergon.core import TaskStatus


# Global execution counters
class Counters:
    validate = 0
    manager_approval = 0
    legal_review = 0
    publish = 0


# ============================================================================
# Custom Error Type
# ============================================================================

class DocumentError(Exception):
    """Custom domain error for document approval workflow."""

    def is_retryable(self) -> bool:
        """Determine if error is retryable."""
        return isinstance(self, InfrastructureError)


class ManagerRejection(DocumentError):
    """Document was rejected by manager (permanent business decision)."""

    def __init__(self, by: str, reason: str):
        self.by = by
        self.reason = reason
        super().__init__(f"Document rejected by {by} - {reason}")


class LegalRejection(DocumentError):
    """Document was rejected by legal team (permanent business decision)."""

    def __init__(self, by: str, reason: str):
        self.by = by
        self.reason = reason
        super().__init__(f"Legal rejected by {by} - {reason}")


class InfrastructureError(DocumentError):
    """Infrastructure or framework error (retryable)."""

    def __init__(self, msg: str):
        super().__init__(f"Infrastructure error: {msg}")


# ============================================================================
# Domain Types
# ============================================================================

@dataclass
class ApprovalDecision:
    """Decision from an approver."""
    approved: bool
    approver: str
    comments: str
    timestamp: int


@dataclass
class ApprovalOutcome:
    """Outcome of an approval step (both approved and rejected are valid outcomes)."""
    approved: bool
    by: str
    message: str

    @staticmethod
    def from_decision(decision: ApprovalDecision) -> 'ApprovalOutcome':
        return ApprovalOutcome(
            approved=decision.approved,
            by=decision.approver,
            message=decision.comments
        )


@dataclass
class DocumentSubmission:
    """Document to be approved."""
    document_id: str
    title: str
    author: str
    content: str


# ============================================================================
# Signal Source Abstraction
# ============================================================================

class SignalSource(Protocol):
    """Protocol for external signal sources."""

    async def poll_for_signal(self, signal_name: str) -> Optional[bytes]:
        """Poll for a signal. Returns signal data if available."""
        ...

    async def consume_signal(self, signal_name: str) -> None:
        """Mark signal as consumed (remove from source)."""
        ...


# ============================================================================
# Simulated User Input Source
# ============================================================================

class SimulatedUserInputSource:
    """Simulates user input with decisions after a delay."""

    def __init__(self):
        self.signals: Dict[str, bytes] = {}
        self._lock = asyncio.Lock()

    async def simulate_user_decision(
        self,
        signal_name: str,
        delay: float,
        approve: bool
    ) -> None:
        """Simulate user making a decision after a delay."""
        await asyncio.sleep(delay)

        decision = ApprovalDecision(
            approved=approve,
            approver="manager@company.com",
            comments="Looks good, approved!" if approve else "Needs revision",
            timestamp=int(datetime.now(timezone.utc).timestamp())
        )

        data = pickle.dumps(decision)

        async with self._lock:
            self.signals[signal_name] = data

        print(f"  [INPUT] User decision received for '{signal_name}'")

    async def poll_for_signal(self, signal_name: str) -> Optional[bytes]:
        """Poll for a signal."""
        async with self._lock:
            return self.signals.get(signal_name)

    async def consume_signal(self, signal_name: str) -> None:
        """Consume a signal."""
        async with self._lock:
            self.signals.pop(signal_name, None)


# ============================================================================
# Document Approval Workflow
# ============================================================================

@dataclass
@flow_type
class DocumentApprovalFlow:
    """Document approval workflow with manager and legal review."""
    submission: DocumentSubmission

    @flow
    async def process(self) -> str:
        """
        Main workflow entry point.

        Steps:
        1. Validate document
        2. Wait for manager approval (SUSPENDS HERE!)
        3. Check manager decision
        4. Wait for legal review (SUSPENDS AGAIN!)
        5. Publish document
        """
        print(f"\n[FLOW] Processing document: {self.submission.title}")
        print(f"       Author: {self.submission.author}")

        # Step 1: Validate document
        await self.validate_document()

        # Step 2: Wait for manager approval (SUSPENDS HERE!)
        manager_outcome = await self.await_manager_approval()

        # Flow decides what rejection MEANS (permanent failure)
        if not manager_outcome.approved:
            print("       [COMPLETE] Document rejected (permanent)")
            raise ManagerRejection(
                by=manager_outcome.by,
                reason=manager_outcome.message
            )

        # Step 3: Wait for legal review (SUSPENDS AGAIN!)
        await self.await_legal_review()

        # Step 4: Publish document
        await self.publish_document()

        return f"Document '{self.submission.title}' approved and published"

    @step
    async def validate_document(self) -> None:
        """Validate document format."""
        Counters.validate += 1
        print("       [STEP] Validating document format...")
        await asyncio.sleep(0.1)
        print("       [OK] Validation passed")

    @step
    async def await_manager_approval(self) -> ApprovalOutcome:
        """
        Wait for manager approval signal.

        This may suspend the flow until signal arrives (or return immediately if cached).
        """
        signal_name = f"manager_approval_{self.submission.document_id}"

        # This suspends until signal arrives (_SuspendExecution must not be caught!)
        decision_bytes = await await_external_signal(signal_name)
        decision: ApprovalDecision = pickle.loads(decision_bytes)

        # Count and log AFTER signal received (only once per flow, not on replay)
        Counters.manager_approval += 1
        print("       [RECEIVED] Manager approval signal received")

        # Convert to outcome - BOTH approved and rejected are successful outcomes
        outcome = ApprovalOutcome.from_decision(decision)

        # Log the outcome
        if outcome.approved:
            print(f"       [OK] Manager approved by {outcome.by} - {outcome.message}")
        else:
            print(f"       [REJECTED] Manager rejected by {outcome.by} - {outcome.message}")

        # Step succeeds with the outcome (cached for replay)
        return outcome

    @step
    async def await_legal_review(self) -> None:
        """Wait for legal review signal."""
        signal_name = f"legal_review_{self.submission.document_id}"

        # Another suspension point (or immediate return if cached)
        # _SuspendExecution must not be caught!
        decision_bytes = await await_external_signal(signal_name)
        decision: ApprovalDecision = pickle.loads(decision_bytes)

        # Count and log AFTER signal received (only once per flow, not on replay)
        Counters.legal_review += 1

        if not decision.approved:
            print(f"       [REJECTED] Legal rejected by {decision.approver} - {decision.comments}")
            raise LegalRejection(
                by=decision.approver,
                reason=decision.comments
            )

        print(f"       [OK] Legal approved by {decision.approver} - {decision.comments}")

    @step
    async def publish_document(self) -> None:
        """Publish the document."""
        Counters.publish += 1
        print("       [STEP] Publishing document...")
        await asyncio.sleep(0.1)
        print("       [OK] Document published")


# ============================================================================
# Main Example
# ============================================================================

async def main():
    """Run the external signal abstraction example."""
    storage = SqliteExecutionLog("data/sig_abstraction.db")
    await storage.connect()
    await storage.reset()

    signal_source = SimulatedUserInputSource()
    scheduler = Scheduler(storage).with_version("v1.0")

    # Create two documents
    doc1 = DocumentSubmission(
        document_id="DOC-001",
        title="Q4 Financial Report",
        author="john.doe@company.com",
        content="Financial summary for Q4..."
    )

    flow1 = DocumentApprovalFlow(submission=doc1)
    task_id_1 = await scheduler.schedule(flow1)

    doc2 = DocumentSubmission(
        document_id="DOC-002",
        title="Policy Update Draft",
        author="jane.smith@company.com",
        content="Proposed policy changes..."
    )

    flow2 = DocumentApprovalFlow(submission=doc2)
    task_id_2 = await scheduler.schedule(flow2)

    # Start worker with signal source
    worker = Worker(storage, "document-processor", enable_timers=False)
    worker = worker.with_signals(signal_source, poll_interval=0.5)
    await worker.register(DocumentApprovalFlow)
    handle = await worker.start()

    # Simulate manager approving DOC-001 after 1 second
    asyncio.create_task(
        signal_source.simulate_user_decision(
            f"manager_approval_{doc1.document_id}",
            1.0,
            True  # approve
        )
    )

    # Simulate legal approving DOC-001 after 2 seconds
    asyncio.create_task(
        signal_source.simulate_user_decision(
            f"legal_review_{doc1.document_id}",
            2.0,
            True  # approve
        )
    )

    # Wait for DOC-001 to complete
    start = asyncio.get_event_loop().time()
    while asyncio.get_event_loop().time() - start < 10:
        scheduled = await storage.get_scheduled_flow(task_id_1)
        if scheduled and scheduled.status in (TaskStatus.COMPLETE, TaskStatus.FAILED):
            break
        await asyncio.sleep(0.1)

    # Simulate manager rejecting DOC-002 after 1 second
    # Key insight: Rejection is an OUTCOME (step succeeds), not an error
    # The step returns ApprovalOutcome with approved=False and is CACHED
    # The flow then raises ManagerRejection (permanent failure)
    # Result: Flow fails immediately with no retries (signal consumed only once)
    asyncio.create_task(
        signal_source.simulate_user_decision(
            f"manager_approval_{doc2.document_id}",
            1.0,
            False  # reject
        )
    )

    # Wait for DOC-002 to fail
    start = asyncio.get_event_loop().time()
    while asyncio.get_event_loop().time() - start < 20:
        scheduled = await storage.get_scheduled_flow(task_id_2)
        if scheduled and scheduled.status in (TaskStatus.COMPLETE, TaskStatus.FAILED):
            break
        await asyncio.sleep(0.1)

    await handle.shutdown()
    await storage.close()

    print(f"\n[PASS] Example completed!")
    print(f"  Validate count: {Counters.validate}")
    print(f"  Manager approval count: {Counters.manager_approval}")
    print(f"  Legal review count: {Counters.legal_review}")
    print(f"  Publish count: {Counters.publish}")


if __name__ == "__main__":
    asyncio.run(main())
