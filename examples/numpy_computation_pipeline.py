"""
Scientific Computation Pipeline - Advantages over Celery/RQ

Demonstrates Ergon's key advantages for data science workloads:
1. Durable execution - steps aren't lost on worker failure
2. Step caching - expensive computations run exactly once
3. Built-in retry policies with exponential backoff
4. Parent-child flow invocation with result passing

Scenario:
- Matrix analysis pipeline (SVD + Eigendecomposition)
- Parallel batch processing via child flows
- Simulated transient failures with automatic retry
- O(n³) operations cached to prevent recomputation

Run:
    PYTHONPATH=src python3 examples/numpy_computation_pipeline.py

Comparison to Celery/RQ:
- Celery: Worker crash loses all in-flight computation
- Ergon:  Automatic resume from last completed step

- Celery: Recompute entire task on retry
- Ergon:  Step caching prevents redundant computation

- Celery: Manual retry logic in every task
- Ergon:  Declarative retry_policy on @flow decorator

- Celery: Manual parent-child coordination via task IDs
- Ergon:  Built-in self.invoke() with automatic result passing
"""

import asyncio
import hashlib
import logging
import pickle
from dataclasses import dataclass

import numpy as np

from pyergon import Scheduler, Worker, flow, flow_type, step
from pyergon.core import RetryPolicy, TaskStatus
from pyergon.storage.sqlite import SqliteExecutionLog

logging.basicConfig(level=logging.CRITICAL)


# =============================================================================
# COMPUTATION CORE
# =============================================================================


def compute_svd(matrix: np.ndarray) -> np.ndarray:
    """
    Singular Value Decomposition - O(n³) complexity.

    Ergon Advantage: Cached via @step decorator.
    Celery Limitation: Recomputed on every retry.
    """
    U, S, Vt = np.linalg.svd(matrix, full_matrices=False)  # noqa: N806
    return S


def compute_eigenvalues(matrix: np.ndarray) -> np.ndarray:
    """
    Eigendecomposition - O(n³) complexity.

    Ergon Advantage: Automatic retry with exponential backoff.
    Celery Limitation: Manual retry logic required.
    """
    eigenvalues, _ = np.linalg.eig(matrix)
    return np.abs(eigenvalues)


# =============================================================================
# FAILURE SIMULATION
# =============================================================================

ATTEMPT_TRACKER = {"count": 0}


def simulate_transient_failure():
    """
    Simulate transient failures (network, OOM, etc.).

    Demonstrates automatic retry behavior.
    """
    ATTEMPT_TRACKER["count"] += 1
    if ATTEMPT_TRACKER["count"] <= 2:
        raise RuntimeError("Simulated network timeout")


# =============================================================================
# CHILD FLOW: Matrix Batch Processing
# =============================================================================


@dataclass
class MatrixResult:
    """Analysis result from single batch."""

    batch_id: int
    singular_values: list[float]
    eigenvalues: list[float]


@dataclass
@flow_type(invokable=MatrixResult)
class MatrixBatchFlow:
    """
    Process single matrix batch with expensive operations.

    Ergon Advantage: Parent-child flow invocation with result passing.
    Celery Limitation: Manual coordination via callbacks.
    """

    batch_id: int
    matrix_size: int

    @step
    async def generate_matrix(self) -> np.ndarray:
        """
        Generate deterministic random matrix.

        Step Caching: Runs exactly once per batch_id.
        """
        np.random.seed(self.batch_id)
        return np.random.randn(self.matrix_size, self.matrix_size)

    @step
    async def analyze_matrix(self, matrix: np.ndarray) -> tuple:
        """
        Perform expensive matrix analysis.

        Step Caching: If this succeeds but next step fails, not recomputed.
        Celery: Would recompute from scratch.
        """
        # Simulate transient failure for batch 1
        if self.batch_id == 1:
            simulate_transient_failure()

        singular_values = compute_svd(matrix)
        eigenvalues = compute_eigenvalues(matrix)
        return singular_values, eigenvalues

    @flow(retry_policy=RetryPolicy.STANDARD)
    async def process(self) -> MatrixResult:
        """
        Main batch processing entry point.

        Durable Execution: Worker crash resumes from last completed step.
        """
        matrix = await self.generate_matrix()
        singular_values, eigenvalues = await self.analyze_matrix(matrix)

        return MatrixResult(
            batch_id=self.batch_id,
            singular_values=singular_values.tolist(),
            eigenvalues=eigenvalues.tolist(),
        )


# =============================================================================
# PARENT FLOW: Pipeline Orchestration
# =============================================================================


@dataclass
@flow_type
class ScientificPipeline:
    """
    Orchestrate parallel matrix analysis pipeline.

    Demonstrates all 4 key advantages:
    1. Durable execution via step caching
    2. Step caching for expensive operations
    3. Retry policies via decorator
    4. Parent-child invocation with result passing
    """

    experiment_id: str
    num_batches: int
    matrix_size: int

    @step
    async def initialize(self) -> dict:
        """
        Pipeline configuration.

        Step Caching: Never recomputed even if pipeline retries.
        """
        return {
            "experiment_id": self.experiment_id,
            "batches": self.num_batches,
            "matrix_size": self.matrix_size,
        }

    @step
    async def process_batches(self, config: dict) -> list[MatrixResult]:
        """
        Process batches via child flow invocation.

        Parent-Child Invocation: Each batch is separate child flow.
        Ergon: Built-in with self.invoke()
        Celery: Manual coordination via group/chord primitives
        """
        results = []
        for batch_id in range(self.num_batches):
            child_flow = MatrixBatchFlow(batch_id=batch_id, matrix_size=self.matrix_size)
            result = await self.invoke(child_flow).result()
            results.append(result)

        return results

    @step
    async def aggregate_results(self, results: list[MatrixResult]) -> dict:
        """
        Aggregate analysis results.

        Step Caching: Even if this fails, batches aren't reprocessed.
        Celery: Manual checkpointing required.
        """
        all_sv = []
        all_ev = []

        for result in results:
            all_sv.extend(result.singular_values)
            all_ev.extend(result.eigenvalues)

        return {
            "total_batches": len(results),
            "mean_singular_value": float(np.mean(all_sv)),
            "max_singular_value": float(np.max(all_sv)),
            "mean_eigenvalue": float(np.mean(all_ev)),
            "max_eigenvalue": float(np.max(all_ev)),
            "result_hash": hashlib.md5(pickle.dumps(all_sv)).hexdigest()[:8],
        }

    @flow(
        retry_policy=RetryPolicy(
            max_attempts=5, initial_delay_ms=500, backoff_multiplier=2.0, max_delay_ms=5000
        )
    )
    async def run(self) -> dict:
        """
        Main pipeline orchestration.

        Retry Policy: Automatic exponential backoff on failure.
        Celery: Manual retry logic in every task.
        """
        config = await self.initialize()
        results = await self.process_batches(config)
        summary = await self.aggregate_results(results)
        return summary


# =============================================================================
# MAIN EXECUTION
# =============================================================================


async def main():
    storage = SqliteExecutionLog("data/numpy_pipeline.db")
    await storage.connect()
    await storage.reset()

    worker = Worker(storage, "worker-1").with_poll_interval(0.1)
    await worker.register(ScientificPipeline)
    await worker.register(MatrixBatchFlow)
    worker_handle = await worker.start()

    scheduler = Scheduler(storage).with_version("v1.0")
    task_id = await scheduler.schedule(
        ScientificPipeline(experiment_id="EXP-2024-001", num_batches=3, matrix_size=50)
    )

    status_notify = storage.status_notify()
    try:
        await asyncio.wait_for(_wait_for_completion(storage, task_id, status_notify), timeout=30.0)
    except TimeoutError:
        print("[Warning] Timeout waiting for completion")

    task = await storage.get_scheduled_flow(task_id)
    if task:
        print(f"{task_id}: {task.status.name} (retries: {task.retry_count})")

        # Retrieve result from storage
        inv = await storage.get_invocation(task.flow_id, 0)
        if inv and inv.return_value:
            import pickle

            try:
                result = pickle.loads(inv.return_value)
                if isinstance(result, dict):
                    print(f"  Batches: {result['total_batches']}")
                    print(f"  Mean singular value: {result['mean_singular_value']:.4f}")
                    print(f"  Max singular value: {result['max_singular_value']:.4f}")
                    print(f"  Mean eigenvalue: {result['mean_eigenvalue']:.4f}")
                    print(f"  Max eigenvalue: {result['max_eigenvalue']:.4f}")
                    print(f"  Result hash: {result['result_hash']}")
            except Exception as e:
                print(f"  Error loading result: {e}")

        if task.error_message:
            print(f"  Error: {task.error_message}")

    await worker_handle.shutdown()
    await storage.close()


async def _wait_for_completion(storage, task_id: str, status_notify: asyncio.Event):
    """Wait for flow to complete using event-driven notifications."""
    while True:
        task = await storage.get_scheduled_flow(task_id)
        if task and task.status in (TaskStatus.COMPLETE, TaskStatus.FAILED):
            break
        await status_notify.wait()
        status_notify.clear()


if __name__ == "__main__":
    asyncio.run(main())
