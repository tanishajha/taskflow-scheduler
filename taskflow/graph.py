"""Dependency-graph helpers: validation, cycle detection, ordering."""
from collections import deque
from typing import Dict, List, Set

from .task import Task


class GraphError(Exception):
    pass


class CycleError(GraphError):
    """The task dependencies contain a cycle, so no valid run order exists."""


class UnknownDependencyError(GraphError):
    """A task depends on an id that was never registered."""


def validate(tasks: Dict[str, Task]) -> None:
    for task in tasks.values():
        for dep in task.depends_on:
            if dep not in tasks:
                raise UnknownDependencyError(
                    f"task '{task.task_id}' depends on unknown task '{dep}'"
                )


def dependents_map(tasks: Dict[str, Task]) -> Dict[str, Set[str]]:
    """Reverse edges: task id -> ids of tasks that depend on it."""
    out: Dict[str, Set[str]] = {tid: set() for tid in tasks}
    for task in tasks.values():
        for dep in task.depends_on:
            out[dep].add(task.task_id)
    return out


def topological_order(tasks: Dict[str, Task]) -> List[str]:
    """Kahn's algorithm, O(V+E). Ties broken alphabetically for determinism."""
    validate(tasks)
    dependents = dependents_map(tasks)
    indegree = {tid: len(set(t.depends_on)) for tid, t in tasks.items()}
    queue = deque(sorted(tid for tid, n in indegree.items() if n == 0))
    order: List[str] = []
    while queue:
        tid = queue.popleft()
        order.append(tid)
        for child in sorted(dependents[tid]):
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)
    if len(order) != len(tasks):
        stuck = sorted(set(tasks) - set(order))
        raise CycleError(f"dependency cycle involving: {', '.join(stuck)}")
    return order


def descendants(start: str, dependents: Dict[str, Set[str]]) -> Set[str]:
    """Every task that directly or transitively depends on `start`."""
    seen: Set[str] = set()
    stack = list(dependents[start])
    while stack:
        node = stack.pop()
        if node not in seen:
            seen.add(node)
            stack.extend(dependents[node])
    return seen
