import unittest

from taskflow.graph import (CycleError, UnknownDependencyError, dependents_map,
                            descendants, topological_order, validate)
from taskflow.task import Task


def make(spec):
    return {tid: Task(tid, lambda: None, tuple(deps)) for tid, deps in spec.items()}


class GraphTests(unittest.TestCase):
    def test_topological_order_respects_dependencies(self):
        tasks = make({"a": [], "b": ["a"], "c": ["a"], "d": ["b", "c"]})
        order = topological_order(tasks)
        self.assertEqual(order[0], "a")
        self.assertEqual(order[-1], "d")
        self.assertLess(order.index("a"), order.index("b"))
        self.assertLess(order.index("c"), order.index("d"))

    def test_cycle_detected(self):
        with self.assertRaises(CycleError):
            topological_order(make({"a": ["b"], "b": ["a"]}))

    def test_self_dependency_is_a_cycle(self):
        with self.assertRaises(CycleError):
            topological_order(make({"a": ["a"]}))

    def test_unknown_dependency(self):
        with self.assertRaises(UnknownDependencyError):
            validate(make({"a": ["ghost"]}))

    def test_descendants_are_transitive(self):
        tasks = make({"a": [], "b": ["a"], "c": ["b"], "x": []})
        self.assertEqual(descendants("a", dependents_map(tasks)), {"b", "c"})
        self.assertEqual(descendants("x", dependents_map(tasks)), set())

    def test_duplicate_dependency_entries_are_harmless(self):
        tasks = make({"a": [], "b": ["a", "a"]})
        self.assertEqual(topological_order(tasks), ["a", "b"])
