import unittest

from taskflow.states import (ALLOWED_TRANSITIONS, TERMINAL_STATES,
                             InvalidTransition, TaskState, check_transition)
from taskflow.task import Task


class StateMachineTests(unittest.TestCase):
    def test_happy_path_transitions(self):
        check_transition(TaskState.PENDING, TaskState.RUNNING)
        check_transition(TaskState.RUNNING, TaskState.SUCCEEDED)

    def test_retry_loop_is_allowed(self):
        check_transition(TaskState.RUNNING, TaskState.RETRYING)
        check_transition(TaskState.RETRYING, TaskState.RUNNING)

    def test_illegal_transitions_raise(self):
        for cur, new in [(TaskState.PENDING, TaskState.SUCCEEDED),
                         (TaskState.SUCCEEDED, TaskState.RUNNING),
                         (TaskState.FAILED, TaskState.RETRYING),
                         (TaskState.RUNNING, TaskState.SKIPPED)]:
            with self.subTest(cur=cur, new=new):
                with self.assertRaises(InvalidTransition):
                    check_transition(cur, new)

    def test_terminal_states_have_no_exits(self):
        self.assertEqual(TERMINAL_STATES,
                         {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.SKIPPED})
        for s in TERMINAL_STATES:
            self.assertEqual(ALLOWED_TRANSITIONS[s], set())

    def test_task_transition_updates_state(self):
        t = Task("a", lambda: None)
        t.transition(TaskState.RUNNING)
        self.assertIs(t.state, TaskState.RUNNING)
        with self.assertRaises(InvalidTransition):
            t.transition(TaskState.PENDING)
