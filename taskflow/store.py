"""SQLite-backed audit trail of every task state change."""
import sqlite3
import threading
import time
from typing import Dict, List, Optional, Tuple


class SQLiteTaskStore:
    def __init__(self, path: str = ":memory:"):
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._lock = threading.Lock()  # worker threads share one connection
        with self._lock:
            self._conn.execute(
                """CREATE TABLE IF NOT EXISTS task_events (
                       id      INTEGER PRIMARY KEY AUTOINCREMENT,
                       run_id  TEXT NOT NULL,
                       task_id TEXT NOT NULL,
                       state   TEXT NOT NULL,
                       attempt INTEGER NOT NULL,
                       error   TEXT,
                       ts      REAL NOT NULL)"""
            )
            self._conn.commit()

    def record(self, run_id: str, task_id: str, state: str,
               attempt: int = 0, error: Optional[str] = None) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO task_events (run_id, task_id, state, attempt, error, ts) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (run_id, task_id, state, attempt, error, time.time()),
            )
            self._conn.commit()

    def history(self, run_id: str, task_id: str) -> List[str]:
        """Ordered list of states a task went through in a run."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT state FROM task_events WHERE run_id=? AND task_id=? ORDER BY id",
                (run_id, task_id),
            ).fetchall()
        return [r[0] for r in rows]

    def final_states(self, run_id: str) -> Dict[str, str]:
        """Latest recorded state per task in a run."""
        with self._lock:
            rows: List[Tuple[str, str]] = self._conn.execute(
                """SELECT task_id, state FROM task_events
                   WHERE id IN (SELECT MAX(id) FROM task_events
                                WHERE run_id=? GROUP BY task_id)""",
                (run_id,),
            ).fetchall()
        return dict(rows)

    def close(self) -> None:
        with self._lock:
            self._conn.close()
