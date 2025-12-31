"""edge_idempotency.py — Phase 24B (Edge)

Durable idempotency ledger to prevent double execution of the same Bus command id
across retries, lease-expiry replays, and restarts.

Key
- (agent_id, command_id)

States
- STARTED: written before execution (safe against double-trade)
- DONE: written after execution result/receipt is produced

Conservative by design
- If STARTED exists, we treat as already-executed and do NOT execute again.
  This can rarely block a command if a crash happens right before execution,
  but it will never double-trade.

Storage
- SQLite file at IDEMPOTENCY_DB_PATH (default ./idempotency.db)
"""

from __future__ import annotations

import os
import sqlite3
import time
from contextlib import contextmanager
from typing import Optional, Tuple

IDEMPOTENCY_DB_PATH = os.getenv("IDEMPOTENCY_DB_PATH", "./idempotency.db")


@contextmanager
def _cx():
    c = sqlite3.connect(IDEMPOTENCY_DB_PATH)
    try:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS idempotency (
                agent_id   TEXT NOT NULL,
                command_id TEXT NOT NULL,
                state      TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (agent_id, command_id)
            );
            """
        )
        c.commit()
        yield c
        c.commit()
    finally:
        try:
            c.close()
        except Exception:
            pass


def claim(agent_id: str, command_id: str) -> Tuple[bool, str]:
    now = int(time.time())
    with _cx() as c:
        cur = c.cursor()
        cur.execute(
            "SELECT state FROM idempotency WHERE agent_id=? AND command_id=?",
            (agent_id, command_id),
        )
        row = cur.fetchone()
        if row:
            return False, str(row[0] or "UNKNOWN")
        cur.execute(
            "INSERT OR REPLACE INTO idempotency(agent_id,command_id,state,created_at,updated_at) VALUES (?,?,?,?,?)",
            (agent_id, command_id, "STARTED", now, now),
        )
        return True, "STARTED"


def mark_done(agent_id: str, command_id: str) -> None:
    now = int(time.time())
    with _cx() as c:
        c.execute(
            "UPDATE idempotency SET state=?, updated_at=? WHERE agent_id=? AND command_id=?",
            ("DONE", now, agent_id, command_id),
        )


def seen(agent_id: str, command_id: str) -> Optional[str]:
    with _cx() as c:
        cur = c.cursor()
        cur.execute(
            "SELECT state FROM idempotency WHERE agent_id=? AND command_id=?",
            (agent_id, command_id),
        )
        row = cur.fetchone()
        if not row:
            return None
        return str(row[0] or "UNKNOWN")
