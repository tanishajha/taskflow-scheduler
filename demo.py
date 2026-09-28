"""Small demo: an ETL-style workflow with a flaky step, retried automatically."""
import random

from taskflow import Scheduler, SQLiteTaskStore

random.seed(7)
store = SQLiteTaskStore("demo_audit.db")
s = Scheduler(max_workers=3, store=store,
              backoff=lambda attempt: 0.05 * 2 ** (attempt - 1))


def flaky_fetch():
    if random.random() < 0.6:
        raise ConnectionError("upstream timeout")
    return "rows"


s.add_task("fetch_orders", flaky_fetch, max_retries=4)
s.add_task("fetch_customers", lambda: "rows")
s.add_task("join", lambda: "joined", depends_on=["fetch_orders", "fetch_customers"])
s.add_task("report", lambda: "report.csv", depends_on=["join"])

for task_id, state in s.run().items():
    print(f"{task_id:16} {state.value:10} attempts={s.get_task(task_id).attempts}")
print("audit trail for fetch_orders:", store.history(s.run_id, "fetch_orders"))
