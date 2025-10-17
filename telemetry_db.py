# telemetry_db.py — Edge-side SQLite mirror for balances, receipts, heartbeats
import os, json, sqlite3, time
from typing import Dict, Any, Iterable, Optional

DB_PATH = os.getenv("EDGE_TELEMETRY_DB", "nova_telemetry.db")

PRAGMAS = [
    "PRAGMA journal_mode=WAL;",
    "PRAGMA synchronous=NORMAL;",
    "PRAGMA temp_store=MEMORY;",
    "PRAGMA foreign_keys=ON;",
]

SCHEMA = """
CREATE TABLE IF NOT EXISTS receipts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  cmd_id TEXT,
  venue TEXT,
  symbol TEXT,
  requested_symbol TEXT,
  resolved_symbol TEXT,
  side TEXT,
  status TEXT,
  order_id TEXT,
  result_json TEXT,
  created_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_receipts_created ON receipts(created_at);

CREATE TABLE IF NOT EXISTS balances (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  venue TEXT,
  asset TEXT,
  free REAL,
  ts INTEGER
);
CREATE INDEX IF NOT EXISTS idx_balances_ts ON balances(ts);
CREATE UNIQUE INDEX IF NOT EXISTS uniq_bal ON balances(venue,asset,ts);

CREATE TABLE IF NOT EXISTS heartbeats (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  agent TEXT,
  ok INTEGER,
  latency_ms INTEGER,
  ts INTEGER
);
CREATE INDEX IF NOT EXISTS idx_hb_ts ON heartbeats(ts);
"""

# Add near top
HEADLINE = {"USDT","USDC","USD","BTC","XBT","ETH"}

def _clean_balances(bal_map):
    out = {}
    for k, v in (bal_map or {}).items():
        k_up = (k or "").upper()
        try:
            val = float(v or 0.0)
        except Exception:
            val = 0.0
        if val != 0.0 or k_up in HEADLINE:
            out[k_up] = val
    return out

def _conn():
    first = not os.path.exists(DB_PATH)
    con = sqlite3.connect(DB_PATH, isolation_level=None, timeout=10)
    con.row_factory = sqlite3.Row
    for p in PRAGMAS: con.execute(p)
    if first:
        for stmt in filter(None, SCHEMA.split(";")):
            s = stmt.strip()
            if s: con.execute(s + ";")
    return con

def log_receipt(*, cmd_id: str, receipt: Dict[str, Any]):
    con = _conn()
    now = int(time.time())
    con.execute(
        "INSERT INTO receipts(cmd_id,venue,symbol,requested_symbol,resolved_symbol,side,status,order_id,result_json,created_at) "
        "VALUES(?,?,?,?,?,?,?,?,?,?)",
        (
            str(cmd_id),
            receipt.get("venue"),
            receipt.get("symbol"),
            receipt.get("requested_symbol"),
            receipt.get("resolved_symbol"),
            receipt.get("side"),
            receipt.get("status"),
            receipt.get("txid") or receipt.get("order_id"),
            json.dumps(receipt, separators=(",", ":"), ensure_ascii=False),
            now,
        ),
    )

def upsert_balances(venue: str, bal_map: Dict[str, float], ts: Optional[int] = None):
    con = _conn()
    t = int(ts or time.time())
    bal_map = _clean_balances(bal_map)
    for asset, free in (bal_map or {}).items():
        if asset is None: continue
        con.execute(
            "INSERT OR REPLACE INTO balances(venue,asset,free,ts) VALUES(?,?,?,?)",
            (venue, str(asset).upper(), float(free or 0.0), t),
        )

def log_heartbeat(agent: str, ok: bool, latency_ms: int = 0, ts: Optional[int] = None):
    con = _conn()
    t = int(ts or time.time())
    con.execute(
        "INSERT INTO heartbeats(agent,ok,latency_ms,ts) VALUES(?,?,?,?)",
        (agent, 1 if ok else 0, int(latency_ms or 0), t),
    )

def aggregates(last_seconds: int = 24 * 3600) -> Dict[str, Any]:
    con = _conn()
    cutoff = int(time.time()) - int(last_seconds)
    # trades count by venue
    trades = {}
    for r in con.execute("SELECT venue, COUNT(*) c FROM receipts WHERE created_at >= ? GROUP BY venue", (cutoff,)):
        trades[r["venue"]] = int(r["c"])
    # last balances snapshot per venue
    bals = {}
    # get max ts per venue
    last_ts = {r["venue"]: r["mx"] for r in con.execute("SELECT venue, MAX(ts) mx FROM balances GROUP BY venue")}
    for v, ts in last_ts.items():
        bals[v] = {r["asset"]: r["free"] for r in con.execute(
            "SELECT asset, free FROM balances WHERE venue=? AND ts=?", (v, ts)
        )}
    # last heartbeat
    hb = con.execute("SELECT agent, ok, latency_ms, ts FROM heartbeats ORDER BY ts DESC LIMIT 1").fetchone()
    hb_row = dict(hb) if hb else None
    return {"trades_24h": trades, "last_balances": bals, "last_heartbeat": hb_row}
