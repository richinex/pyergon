"""
External signal handling for document approval workflow.

Demonstrates signal source abstraction with custom domain errors.
Worker suspends flows awaiting external signals (manager/legal approval).

Run:
    PYTHONPATH=src python examples/external_signal_abstraction.py
"""

import asyncio
import pickle
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from pyergon import Scheduler, Worker, flow, flow_type, step
from pyergon.core import TaskStatus
from pyergon.executor.signal import await_external_signal
from pyergon.storage.sqlite import SqliteExecutionLog


class Counters:
    validate = 0
    manager_approval = 0
    legal_review = 0
    publish = 0


class DocumentError(Exception):
    """Domain error for document approval workflow."""

    def is_retryable(self) -> bool:
        return isinstance(self, InfrastructureError)


class ManagerRejectionError(DocumentError):
    """Manager rejected document."""

    def __init__(self, by: str, reason: str):
        self.by = by
        self.reason = reason
        super().__init__(f"Document rejected by {by} - {reason}")


class LegalRejectionError(DocumentError):
    """Legal team rejected document."""

    def __init__(self, by: str, reason: str):
        self.by = by
        self.reason = reason
        super().__init__(f"Legal rejected by {by} - {reason}")


class InfrastructureError(DocumentError):
    """Infrastructure error (retryable)."""

    def __init__(self, msg: str):
        super().__init__(f"Infrastructure error: {msg}")


@dataclass
class ApprovalDecision:
    """Approval decision from reviewer."""

    approved: bool
    approver: str
    comments: str
    timestamp: int


@dataclass
class ApprovalOutcome:
    """Approval outcome (approved or rejected)."""

    approved: bool
    by: str
    message: str

    @staticmethod
    def from_decision(decision: ApprovalDecision) -> "ApprovalOutcome":
        return ApprovalOutcome(
            approved=decision.approved, by=decision.approver, message=decision.comments
        )


@dataclass
class DocumentSubmission:
    """Document submission for approval."""

    document_id: str
    title: str
    author: str
    content: str


class SignalSource(Protocol):
    """External signal source protocol."""

    async def poll_for_signal(self, signal_name: str) -> bytes | None:
        """Poll for signal data."""
        ...

    async def consume_signal(self, signal_name: str) -> None:
        """Consume signal from source."""
        ...


class SimulatedUserInputSource:
    """Simulated user input source for testing."""

    def __init__(self):
        self.signals: dict[str, bytes] = {}
        self._lock = asyncio.Lock()

    async def simulate_user_decision(self, signal_name: str, delay: float, approve: bool) -> None:
        """Simulate user decision after delay."""
        await asyncio.sleep(delay)

        decision = ApprovalDecision(
            approved=approve,
            approver="manager@company.com",
            comments="Looks good, approved!" if approve else "Needs revision",
            timestamp=int(datetime.now(UTC).timestamp()),
        )

        async with self._lock:
            self.signals[signal_name] = pickle.dumps(decision)

    async def poll_for_signal(self, signal_name: str) -> bytes | None:
        """Poll for signal data."""
        async with self._lock:
            return self.signals.get(signal_name)

    async def consume_signal(self, signal_name: str) -> None:
        """Consume signal from source."""
        async with self._lock:
            self.signals.pop(signal_name, None)


@dataclass
@flow_type
class DocumentApprovalFlow:
    """Document approval workflow with external signal suspension."""

    submission: DocumentSubmission

    @flow
    async def process(self) -> str:
        """Process document through approval workflow."""
        await self.validate_document()
        manager_outcome = await self.await_manager_approval()

        if not manager_outcome.approved:
            raise ManagerRejectionError(by=manager_outcome.by, reason=manager_outcome.message)

        await self.await_legal_review()
        await self.publish_document()

        return f"Document '{self.submission.title}' approved and published"

    @step
    async def validate_document(self) -> None:
        """Validate document format."""
        Counters.validate += 1
        await asyncio.sleep(0.1)

    @step
    async def await_manager_approval(self) -> ApprovalOutcome:
        """Wait for manager approval signal (suspends flow)."""
        signal_name = f"manager_approval_{self.submission.document_id}"
        decision_bytes = await await_external_signal(signal_name)
        decision: ApprovalDecision = pickle.loads(decision_bytes)

        Counters.manager_approval += 1
        return ApprovalOutcome.from_decision(decision)

    @step
    async def await_legal_review(self) -> None:
        """Wait for legal review signal (suspends flow)."""
        signal_name = f"legal_review_{self.submission.document_id}"
        decision_bytes = await await_external_signal(signal_name)
        decision: ApprovalDecision = pickle.loads(decision_bytes)

        Counters.legal_review += 1

        if not decision.approved:
            raise LegalRejectionError(by=decision.approver, reason=decision.comments)

    @step
    async def publish_document(self) -> None:
        """Publish the document."""
        Counters.publish += 1
        await asyncio.sleep(0.1)


async def main():
    """Run document approval workflow with external signals."""
    storage = SqliteExecutionLog("data/sig_abstraction.db")
    await storage.connect()
    await storage.reset()

    signal_source = SimulatedUserInputSource()
    scheduler = Scheduler(storage).with_version("v1.0")

    doc1 = DocumentSubmission(
        document_id="DOC-001",
        title="Q4 Financial Report",
        author="john.doe@company.com",
        content="Financial summary for Q4...",
    )
    task_id_1 = await scheduler.schedule(DocumentApprovalFlow(submission=doc1))

    doc2 = DocumentSubmission(
        document_id="DOC-002",
        title="Policy Update Draft",
        author="jane.smith@company.com",
        content="Proposed policy changes...",
    )
    task_id_2 = await scheduler.schedule(DocumentApprovalFlow(submission=doc2))

    worker = Worker(storage, "document-processor", enable_timers=False)
    worker = worker.with_signals(signal_source, poll_interval=0.5)
    await worker.register(DocumentApprovalFlow)
    handle = await worker.start()

    asyncio.create_task(
        signal_source.simulate_user_decision(f"manager_approval_{doc1.document_id}", 1.0, True)
    )
    asyncio.create_task(
        signal_source.simulate_user_decision(f"legal_review_{doc1.document_id}", 2.0, True)
    )

    start = asyncio.get_event_loop().time()
    while asyncio.get_event_loop().time() - start < 10:
        scheduled = await storage.get_scheduled_flow(task_id_1)
        if scheduled and scheduled.status in (TaskStatus.COMPLETE, TaskStatus.FAILED):
            break
        await asyncio.sleep(0.1)

    asyncio.create_task(
        signal_source.simulate_user_decision(f"manager_approval_{doc2.document_id}", 1.0, False)
    )

    start = asyncio.get_event_loop().time()
    while asyncio.get_event_loop().time() - start < 20:
        scheduled = await storage.get_scheduled_flow(task_id_2)
        if scheduled and scheduled.status in (TaskStatus.COMPLETE, TaskStatus.FAILED):
            break
        await asyncio.sleep(0.1)

    await handle.shutdown()
    await storage.close()

    print(
        f"Counters: validate={Counters.validate}, "
        f"manager={Counters.manager_approval}, "
        f"legal={Counters.legal_review}, publish={Counters.publish}"
    )


if __name__ == "__main__":
    asyncio.run(main())
