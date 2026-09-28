from dataclasses import dataclass
from typing import Any, Callable, Optional, Tuple

from .states import TaskState, check_transition


@dataclass
class Task:
    task_id: str
    fn: Callable[[], Any]
    depends_on: Tuple[str, ...] = ()
    max_retries: int = 0
    state: TaskState = TaskState.PENDING
    attempts: int = 0
    error: Optional[str] = None
    result: Any = None

    def transition(self, new_state: TaskState) -> None:
        check_transition(self.state, new_state)
        self.state = new_state
