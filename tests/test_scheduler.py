import threading
import unittest

from taskflow import (CycleError, Scheduler, SQLiteTaskStore, TaskState,
                      UnknownDependencyError)


class SchedulerTests(unittest.TestCase):
    def test_runs_in_dependency_order(self):
        order, lock = [], threading.Lock()

        def job(name):
            def _run():
                with lock:
                    order.append(name)
            return _run

        s = Scheduler(max_workers=4)
        s.add_task("d", job("d"), depends_on=["b", "c"])
        s.add_task("b", job("b"), depends_on=["a"])
        s.add_task("c", job("c"), depends_on=["a"])
        s.add_task("a", job("a"))
        result = s.run()
        self.assertTrue(all(st is TaskState.SUCCEEDED for st in result.values()))
        self.assertEqual(order[0], "a")
        self.assertEqual(order[-1], "d")

    def test_independent_tasks_run_concurrently(self):
        barrier = threading.Barrier(2, timeout=2)
        s = Scheduler(max_workers=2)
        s.add_task("x", barrier.wait)
        s.add_task("y", barrier.wait)
        result = s.run()   # only completes if both tasks are in flight together
        self.assertEqual(set(result.values()), {TaskState.SUCCEEDED})

    def test_single_worker_cannot_satisfy_barrier(self):
        barrier = threading.Barrier(2, timeout=0.3)
        s = Scheduler(max_workers=1)
        s.add_task("x", barrier.wait)
        s.add_task("y", barrier.wait)
        result = s.run()
        self.assertIn(TaskState.FAILED, result.values())

    def test_retry_then_succeed_with_exponential_backoff(self):
        calls, sleeps = [], []

        def flaky():
            calls.append(1)
            if len(calls) < 3:
                raise RuntimeError("transient")
            return "ok"

        s = Scheduler(backoff=lambda attempt: 0.1 * 2 ** (attempt - 1),
                      sleep=sleeps.append)
        s.add_task("flaky", flaky, max_retries=2)
        result = s.run()
        task = s.get_task("flaky")
        self.assertIs(result["flaky"], TaskState.SUCCEEDED)
        self.assertEqual(task.attempts, 3)
        self.assertEqual(task.result, "ok")
        self.assertIsNone(task.error)
        self.assertEqual(sleeps, [0.1, 0.2])

    def test_retries_exhausted_marks_failed(self):
        def boom():
            raise ValueError("boom")

        s = Scheduler(sleep=lambda _: None)
        s.add_task("bad", boom, max_retries=1)
        result = s.run()
        task = s.get_task("bad")
        self.assertIs(result["bad"], TaskState.FAILED)
        self.assertEqual(task.attempts, 2)
        self.assertIn("boom", task.error)

    def test_failure_skips_downstream_but_not_independent_branch(self):
        ran = []
        s = Scheduler()
        s.add_task("a", lambda: 1 / 0)
        s.add_task("b", lambda: ran.append("b"), depends_on=["a"])
        s.add_task("c", lambda: ran.append("c"), depends_on=["b"])
        s.add_task("x", lambda: ran.append("x"))
        result = s.run()
        self.assertIs(result["a"], TaskState.FAILED)
        self.assertIs(result["b"], TaskState.SKIPPED)
        self.assertIs(result["c"], TaskState.SKIPPED)
        self.assertIs(result["x"], TaskState.SUCCEEDED)
        self.assertEqual(ran, ["x"])

    def test_diamond_with_one_failed_parent_is_skipped(self):
        ran = []
        s = Scheduler()
        s.add_task("a", lambda: None)
        s.add_task("b", lambda: None, depends_on=["a"])
        s.add_task("c", lambda: 1 / 0, depends_on=["a"])
        s.add_task("d", lambda: ran.append("d"), depends_on=["b", "c"])
        result = s.run()
        self.assertIs(result["b"], TaskState.SUCCEEDED)
        self.assertIs(result["d"], TaskState.SKIPPED)
        self.assertEqual(ran, [])

    def test_cycle_detected_before_anything_runs(self):
        ran = []
        s = Scheduler()
        s.add_task("a", lambda: ran.append("a"), depends_on=["b"])
        s.add_task("b", lambda: ran.append("b"), depends_on=["a"])
        with self.assertRaises(CycleError):
            s.run()
        self.assertEqual(ran, [])

    def test_unknown_dependency_rejected(self):
        s = Scheduler()
        s.add_task("a", lambda: None, depends_on=["nope"])
        with self.assertRaises(UnknownDependencyError):
            s.run()

    def test_input_validation(self):
        with self.assertRaises(ValueError):
            Scheduler(max_workers=0)
        s = Scheduler()
        s.add_task("a", lambda: None)
        with self.assertRaises(ValueError):
            s.add_task("a", lambda: None)
        with self.assertRaises(ValueError):
            s.add_task("b", lambda: None, max_retries=-1)

    def test_many_parallel_tasks_all_complete(self):
        counter, lock = [0], threading.Lock()

        def inc():
            with lock:
                counter[0] += 1

        s = Scheduler(max_workers=8)
        for i in range(200):
            s.add_task(f"t{i}", inc)
        result = s.run()
        self.assertEqual(counter[0], 200)
        self.assertEqual(set(result.values()), {TaskState.SUCCEEDED})


class PersistenceTests(unittest.TestCase):
    def test_audit_trail_records_every_transition(self):
        calls = []

        def flaky():
            calls.append(1)
            if len(calls) < 2:
                raise RuntimeError("once")

        store = SQLiteTaskStore()
        s = Scheduler(store=store, sleep=lambda _: None)
        s.add_task("flaky", flaky, max_retries=1)
        s.add_task("after", lambda: None, depends_on=["flaky"])
        s.run()
        self.assertEqual(store.history(s.run_id, "flaky"),
                         ["PENDING", "RUNNING", "RETRYING", "RUNNING", "SUCCEEDED"])
        self.assertEqual(store.final_states(s.run_id),
                         {"flaky": "SUCCEEDED", "after": "SUCCEEDED"})
        store.close()

    def test_skipped_and_failed_are_persisted(self):
        store = SQLiteTaskStore()
        s = Scheduler(store=store)
        s.add_task("a", lambda: 1 / 0)
        s.add_task("b", lambda: None, depends_on=["a"])
        s.run()
        self.assertEqual(store.final_states(s.run_id), {"a": "FAILED", "b": "SKIPPED"})
        store.close()

    def test_runs_are_isolated_by_run_id(self):
        store = SQLiteTaskStore()
        s1 = Scheduler(store=store)
        s1.add_task("a", lambda: None)
        s1.run()
        s2 = Scheduler(store=store)
        s2.add_task("a", lambda: 1 / 0)
        s2.run()
        self.assertEqual(store.final_states(s1.run_id), {"a": "SUCCEEDED"})
        self.assertEqual(store.final_states(s2.run_id), {"a": "FAILED"})
        store.close()


if __name__ == "__main__":
    unittest.main()
