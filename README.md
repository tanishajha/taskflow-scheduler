# Taskflow — Dependency-Aware Workflow Scheduler

A small Python library that runs a set of tasks in dependency order, executes independent
tasks concurrently, retries flaky tasks with exponential backoff, isolates failures, and keeps
an SQLite audit trail of every state change. Standard library only (no installs needed).

## Why it exists
Multi-step jobs (ETL, report generation, migrations) are often run by hand or with fragile scripts.
Taskflow expresses them as a DAG so ordering, retries and failure handling are automatic and auditable.

## Design
| Piece | File | Idea |
|---|---|---|
| State machine | `taskflow/states.py` | `PENDING -> RUNNING -> SUCCEEDED / RETRYING -> RUNNING / FAILED`, `PENDING -> SKIPPED`. Illegal moves raise `InvalidTransition`. |
| Graph | `taskflow/graph.py` | Unknown-dependency check, cycle detection via Kahn's algorithm (O(V+E)), transitive descendants. |
| Scheduler | `taskflow/scheduler.py` | `ThreadPoolExecutor` + `wait(FIRST_COMPLETED)` loop; a task becomes ready when its dependency count hits 0. |
| Store | `taskflow/store.py` | SQLite `task_events` table; one connection guarded by a lock for worker threads. |

**Failure isolation:** when a task fails after its retries, only its descendants are `SKIPPED`; unrelated branches finish normally.

## Run
```bash
python -m unittest discover -v   # 25 tests
python demo.py                   # example workflow with a flaky step
```

## Usage
```python
from taskflow import Scheduler, SQLiteTaskStore

s = Scheduler(max_workers=4, store=SQLiteTaskStore("audit.db"),
              backoff=lambda attempt: 0.1 * 2 ** (attempt - 1))
s.add_task("extract", extract, max_retries=3)
s.add_task("transform", transform, depends_on=["extract"])
s.add_task("load", load, depends_on=["transform"])
print(s.run())
```

## Ideas to extend (good interview talking points)
- Add a `DynamoDBTaskStore` implementing the same `record / history / final_states` methods (boto3).
- Store task artifacts in S3; expose `run()` and status through a small REST API.
- Per-task timeouts, priorities, and cron-style scheduled runs.
