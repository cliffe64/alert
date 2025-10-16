"""SQLite database management utilities for the alerting service."""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional

_DB_PATH_ENV = "ALERT_DB_PATH"
_DEFAULT_DB_FILENAME = "alert.db"
_ALLOWED_BAR_TABLES = {"bars_1m", "bars_5m", "bars_15m"}

_connection_lock = Lock()


def get_db_path() -> str:
    """Return the configured SQLite database path."""
    env_path = os.environ.get(_DB_PATH_ENV)
    if env_path:
        return env_path
    storage_dir = Path(__file__).resolve().parent
    return str(storage_dir / _DEFAULT_DB_FILENAME)


def _connect(db_path: Optional[str] = None) -> sqlite3.Connection:
    path = Path(db_path or get_db_path())
    path.parent.mkdir(parents=True, exist_ok=True)
    # SQLite autocommit is enabled when isolation_level=None. We keep the default
    # behaviour (implicit transactions) to ensure atomicity of batch upserts.
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _execute(query: str, params: Iterable[Any] | Dict[str, Any] | None = None) -> None:
    with _connection_lock:
        with _connect() as conn:
            conn.execute(query, params or [])
            conn.commit()


def _executemany(query: str, seq_of_params: Iterable[Iterable[Any]]) -> None:
    with _connection_lock:
        with _connect() as conn:
            conn.executemany(query, seq_of_params)
            conn.commit()


def _query(
    query: str,
    params: Iterable[Any] | Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    with _connection_lock:
        with _connect() as conn:
            cursor = conn.execute(query, params or [])
            rows = cursor.fetchall()
    return [dict(row) for row in rows]


def _build_upsert_sql(
    table: str,
    payload: Dict[str, Any],
    conflict_columns: Iterable[str],
    skip_update: Iterable[str] | None = None,
) -> tuple[str, list[Any]]:
    columns = list(payload.keys())
    if not columns:
        raise ValueError("payload must include at least one column")
    placeholders = ", ".join(["?" for _ in columns])
    columns_sql = ", ".join(columns)
    skip = set(skip_update or [])
    conflict_set = set(conflict_columns)
    update_columns = [col for col in columns if col not in conflict_set and col not in skip]
    if update_columns:
        update_clause = ", ".join([f"{col}=excluded.{col}" for col in update_columns])
        sql = (
            f"INSERT INTO {table} ({columns_sql}) VALUES ({placeholders}) "
            f"ON CONFLICT({', '.join(conflict_columns)}) DO UPDATE SET {update_clause}"
        )
    else:
        sql = (
            f"INSERT INTO {table} ({columns_sql}) VALUES ({placeholders}) "
            f"ON CONFLICT({', '.join(conflict_columns)}) DO NOTHING"
        )
    params = [payload[col] for col in columns]
    return sql, params


def upsert_bar(table: str, bar: Dict[str, Any]) -> None:
    """Insert or update a bar record in the specified timeframe table."""
    if table not in _ALLOWED_BAR_TABLES:
        raise ValueError(f"Unsupported bars table: {table}")
    if not bar:
        raise ValueError("bar payload is empty")

    sql, params = _build_upsert_sql(
        table,
        bar,
        conflict_columns=("source", "exchange", "chain", "symbol", "close_ts"),
        skip_update={"bid"},
    )
    with _connection_lock:
        with _connect() as conn:
            conn.execute(sql, params)
            conn.commit()


def fetch_bars(
    table: str,
    symbol: str,
    since_ts: Optional[int] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Fetch bars ordered by close_ts ascending."""
    if table not in _ALLOWED_BAR_TABLES:
        raise ValueError(f"Unsupported bars table: {table}")

    query = f"SELECT * FROM {table} WHERE symbol = ?"
    params: List[Any] = [symbol]
    if since_ts is not None:
        query += " AND close_ts >= ?"
        params.append(since_ts)
    query += " ORDER BY close_ts ASC"
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    return _query(query, params)


def insert_event(event: Dict[str, Any]) -> None:
    if "id" not in event:
        raise ValueError("event must include 'id'")
    sql, params = _build_upsert_sql(
        "events",
        event,
        conflict_columns=("id",),
    )
    with _connection_lock:
        with _connect() as conn:
            conn.execute(sql, params)
            conn.commit()


def set_kv(key: str, value: str, updated_at: int) -> None:
    sql = (
        "INSERT INTO kv_state (key, value, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at"
    )
    _execute(sql, (key, value, updated_at))


def get_kv(key: str) -> Optional[Dict[str, Any]]:
    rows = _query("SELECT key, value, updated_at FROM kv_state WHERE key = ?", (key,))
    return rows[0] if rows else None


def upsert_rule(rule: Dict[str, Any]) -> None:
    if "id" not in rule:
        raise ValueError("rule must include 'id'")
    sql, params = _build_upsert_sql(
        "price_alert_rules",
        rule,
        conflict_columns=("id",),
    )
    with _connection_lock:
        with _connect() as conn:
            conn.execute(sql, params)
            conn.commit()


def list_rules(symbol: Optional[str] = None, enabled: Optional[bool] = None) -> List[Dict[str, Any]]:
    query = "SELECT * FROM price_alert_rules"
    conditions: List[str] = []
    params: List[Any] = []
    if symbol is not None:
        conditions.append("symbol = ?")
        params.append(symbol)
    if enabled is not None:
        conditions.append("enabled = ?")
        params.append(1 if enabled else 0)
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY symbol, level"
    return _query(query, params)


def upsert_token(token: Dict[str, Any]) -> None:
    if "id" not in token:
        raise ValueError("token must include 'id'")
    sql, params = _build_upsert_sql(
        "token_registry",
        token,
        conflict_columns=("id",),
    )
    with _connection_lock:
        with _connect() as conn:
            conn.execute(sql, params)
            conn.commit()


def list_tokens(enabled: Optional[bool] = None) -> List[Dict[str, Any]]:
    query = "SELECT * FROM token_registry"
    params: List[Any] = []
    if enabled is not None:
        query += " WHERE enabled = ?"
        params.append(1 if enabled else 0)
    query += " ORDER BY symbol"
    return _query(query, params)


__all__ = [
    "get_db_path",
    "upsert_bar",
    "fetch_bars",
    "insert_event",
    "set_kv",
    "get_kv",
    "upsert_rule",
    "list_rules",
    "upsert_token",
    "list_tokens",
]
