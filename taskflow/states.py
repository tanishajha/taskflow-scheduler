"""Task lifecycle modelled as an explicit state machine."""
from enum import Enum


class TaskState(str, Enum):
    PENDING = "PENDING"      # waiting for dependencies
    RUNNING = "RUNNING"      # currently executing on a worker thread
    RETRYING = "RETRYING"    # failed an attempt, backing off before the next one
    SUCCEEDED = "SUCCEEDED"  # terminal
    FAILED = "FAILED"        # terminal: retries exhausted
    SKIPPED = "SKIPPED"      # terminal: an upstream dependency failed


ALLOWED_TRANSITIONS = {
    TaskState.PENDING: {TaskState.RUNNING, TaskState.SKIPPED},
    TaskState.RUNNING: {TaskState.SUCCEEDED, TaskState.RETRYING, TaskState.FAILED},
    TaskState.RETRYING: {TaskState.RUNNING},
    TaskState.SUCCEEDED: set(),
    TaskState.FAILED: set(),
    TaskState.SKIPPED: set(),
}

TERMINAL_STATES = frozenset(
    s for s, nxt in ALLOWED_TRANSITIONS.items() if not nxt
)


class InvalidTransition(Exception):
    """Raised when a task is moved to a state its current state does not allow."""


def check_transition(current: TaskState, new: TaskState) -> None:
    if new not in ALLOWED_TRANSITIONS[current]:
        raise InvalidTransition(f"{current.value} -> {new.value} is not allowed")
