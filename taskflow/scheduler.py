"""Dependency-aware, multi-threaded task scheduler with retries and an audit trail."""
import time
import uuid
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Any, Callable, Dict, Optional, Sequence

from .graph import dependents_map, descendants, topological_order, validate
from .states import TaskState
from .store import SQLiteTaskStore
from .task import Task


class Scheduler:
    """Runs tasks in dependency order, executing independent tasks concurrently.

    * Each task follows the TaskState state machine.
    * A failing task is retried up to `max_retries` times with `backoff(attempt)` seconds
      of delay; once retries are exhausted it FAILS and every downstream task is SKIPPED,
      while unrelated branches keep running (failure isolation).
    * If a store is given, every state change is persisted for auditing.
    """

    def __init__(self, max_workers: int = 4,
                 store: Optional[SQLiteTaskStore] = None,
                 backoff: Optional[Callable[[int], float]] = None,
                 sleep: Callable[[float], None] = time.sleep):
        if max_workers < 1:
            raise ValueError("max_workers must be >= 1")
        self._max_workers = max_workers
        self._store = store
        self._backoff = backoff or (lambda attempt: 0.0)
        self._sleep = sleep
        self._tasks: Dict[str, Task] = {}
        self.run_id: Optional[str] = None

    def add_task(self, task_id: str, fn: Callable[[], Any],
                 depends_on: Sequence[str] = (), max_retries: int = 0) -> None:
        if task_id in self._tasks:
            raise ValueError(f"duplicate task id '{task_id}'")
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        self._tasks[task_id] = Task(task_id, fn, tuple(depends_on), max_retries)

    def get_task(self, task_id: str) -> Task:
        return self._tasks[task_id]

    def run(self) -> Dict[str, TaskState]:
        validate(self._tasks)
        topological_order(self._tasks)          # fail fast on cycles
        self.run_id = uuid.uuid4().hex[:8]
        for task in self._tasks.values():
            self._log(task)

        dependents = dependents_map(self._tasks)
        remaining = {tid: len(set(t.depends_on)) for tid, t in self._tasks.items()}
        ready = deque(sorted(tid for tid, n in remaining.items() if n == 0))
        in_flight = {}

        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            while ready or in_flight:
                while ready:
                    tid = ready.popleft()
                    in_flight[pool.submit(self._execute, self._tasks[tid])] = tid
                done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                for future in done:
                    tid = in_flight.pop(future)
                    if future.result() is TaskState.SUCCEEDED:
                        for child in sorted(dependents[tid]):
                            remaining[child] -= 1
                            if (remaining[child] == 0
                                    and self._tasks[child].state is TaskState.PENDING):
                                ready.append(child)
                    else:
                        for child in sorted(descendants(tid, dependents)):
                            if self._tasks[child].state is TaskState.PENDING:
                                self._set(self._tasks[child], TaskState.SKIPPED)

        return {tid: t.state for tid, t in self._tasks.items()}

    # ---- internals -------------------------------------------------------
    def _execute(self, task: Task) -> TaskState:
        while True:
            task.attempts += 1
            self._set(task, TaskState.RUNNING)
            try:
                task.result = task.fn()
            except Exception as exc:  # noqa: BLE001 - task code may raise anything
                task.error = f"{type(exc).__name__}: {exc}"
                if task.attempts <= task.max_retries:
                    self._set(task, TaskState.RETRYING)
                    self._sleep(self._backoff(task.attempts))
                    continue
                self._set(task, TaskState.FAILED)
                return task.state
            task.error = None
            self._set(task, TaskState.SUCCEEDED)
            return task.state

    def _set(self, task: Task, new_state: TaskState) -> None:
        task.transition(new_state)
        self._log(task)

    def _log(self, task: Task) -> None:
        if self._store is not None:
            self._store.record(self.run_id, task.task_id, task.state.value,
                               task.attempts, task.error)
