"""Pipeline database — SQLite setup and schema."""

import os
import sqlite3
import threading
import logging
from contextlib import contextmanager
from typing import Optional

logger = logging.getLogger("pipeline.db")

SCHEMA_SQL = """
    CREATE TABLE IF NOT EXISTS workflows (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        version TEXT NOT NULL DEFAULT '1.0.0',
        description TEXT,
        definition TEXT NOT NULL,
        checksum TEXT NOT NULL,
        is_active INTEGER DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(name, version)
    );

    CREATE TABLE IF NOT EXISTS executions (
        id TEXT PRIMARY KEY,
        workflow_id TEXT NOT NULL,
        workflow_version TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        trigger_type TEXT,
        trigger_data TEXT,
        inputs TEXT,
        outputs TEXT,
        error TEXT,
        started_at TEXT,
        completed_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        parent_execution_id TEXT,
        correlation_id TEXT,
        FOREIGN KEY (workflow_id) REFERENCES workflows(id)
    );
    CREATE INDEX IF NOT EXISTS idx_exec_workflow ON executions(workflow_id, status);
    CREATE INDEX IF NOT EXISTS idx_exec_status ON executions(status);

    CREATE TABLE IF NOT EXISTS node_executions (
        id TEXT PRIMARY KEY,
        execution_id TEXT NOT NULL,
        node_id TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        attempt INTEGER DEFAULT 1,
        input_data TEXT,
        output_data TEXT,
        error TEXT,
        healing_log TEXT,
        started_at TEXT,
        completed_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (execution_id) REFERENCES executions(id)
    );
    CREATE INDEX IF NOT EXISTS idx_node_exec ON node_executions(execution_id, status);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_node_exec_unique ON node_executions(execution_id, node_id, attempt);

    CREATE TABLE IF NOT EXISTS timers (
        id TEXT PRIMARY KEY,
        execution_id TEXT,
        node_id TEXT,
        timer_type TEXT NOT NULL,
        fires_at TEXT NOT NULL,
        payload TEXT,
        status TEXT DEFAULT 'pending',
        workflow_id TEXT,
        created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_timers_fires ON timers(status, fires_at);

    CREATE TABLE IF NOT EXISTS webhooks (
        id TEXT PRIMARY KEY,
        workflow_id TEXT NOT NULL,
        secret TEXT,
        is_active INTEGER DEFAULT 1,
        created_at TEXT NOT NULL,
        FOREIGN KEY (workflow_id) REFERENCES workflows(id)
    );

    CREATE TABLE IF NOT EXISTS execution_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        execution_id TEXT NOT NULL,
        node_id TEXT,
        event_type TEXT NOT NULL,
        details TEXT,
        created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_log_exec ON execution_log(execution_id, created_at);

    CREATE TABLE IF NOT EXISTS healing_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        execution_id TEXT NOT NULL,
        node_id TEXT NOT NULL,
        attempt INTEGER NOT NULL,
        error_category TEXT,
        error_message TEXT,
        decision_action TEXT,
        decision_confidence REAL,
        decision_reasoning TEXT,
        decision_raw TEXT,
        outcome TEXT,
        created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_healing_exec ON healing_history(execution_id);
"""


class PipelineDB:
    def __init__(self, config: Optional[dict] = None):
        config = config or {}
        self.db_path = config.get("db_path", os.environ.get("PIPELINE_DB_PATH", "./pipeline.db"))
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._setup()

    def _setup(self):
        c = self._conn
        c.execute("PRAGMA journal_mode = WAL")
        c.execute("PRAGMA synchronous = NORMAL")
        c.execute("PRAGMA busy_timeout = 5000")
        c.execute("PRAGMA temp_store = MEMORY")
        with self.cursor() as cur:
            cur.executescript(SCHEMA_SQL)

    @contextmanager
    def cursor(self):
        with self._lock:
            self._conn.execute("BEGIN")
            try:
                cur = self._conn.cursor()
                yield cur
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def read_cursor(self):
        return self._conn.cursor()

    def close(self):
        try:
            self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            self._conn.close()
        except Exception:
            pass
