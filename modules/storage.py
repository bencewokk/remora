from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator

from .models import OrderBookSnapshot, OrderIntent, OrderResult, WhaleSignal


class Storage:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS book_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_id TEXT NOT NULL,
                    asset_id INTEGER NOT NULL,
                    outcome_side TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    sequence_id INTEGER,
                    is_reset INTEGER NOT NULL DEFAULT 0,
                    best_bid REAL,
                    best_ask REAL,
                    mid_price REAL,
                    bids_json TEXT NOT NULL,
                    asks_json TEXT NOT NULL,
                    raw_message_json TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_book_events_market_time
                ON book_events (market_id, asset_id, timestamp);

                CREATE TABLE IF NOT EXISTS whale_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_id TEXT NOT NULL,
                    asset_id INTEGER NOT NULL,
                    side TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    trigger_type TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    details_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_whale_signals_market_time
                ON whale_signals (market_id, timestamp);

                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_id TEXT NOT NULL,
                    asset_id INTEGER NOT NULL,
                    side TEXT NOT NULL,
                    size_usdh REAL NOT NULL,
                    quantity INTEGER NOT NULL,
                    price REAL NOT NULL,
                    client_order_id TEXT NOT NULL UNIQUE,
                    order_id TEXT,
                    status TEXT NOT NULL,
                    filled_price REAL,
                    paper_trade INTEGER NOT NULL,
                    signal_id INTEGER,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    details_json TEXT NOT NULL,
                    FOREIGN KEY (signal_id) REFERENCES whale_signals (id)
                );

                CREATE INDEX IF NOT EXISTS idx_orders_market_status
                ON orders (market_id, status);

                CREATE TABLE IF NOT EXISTS ledger_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    cash_balance REAL NOT NULL,
                    equity REAL NOT NULL,
                    peak_equity REAL NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            self._ensure_orders_column(connection, "quantity", "INTEGER NOT NULL DEFAULT 0")
            row = connection.execute("SELECT id FROM ledger_state WHERE id = 1").fetchone()
            if row is None:
                now = datetime.utcnow().isoformat()
                connection.execute(
                    """
                    INSERT INTO ledger_state (id, cash_balance, equity, peak_equity, updated_at)
                    VALUES (1, 0.0, 0.0, 0.0, ?)
                    """,
                    (now,),
                )

    @staticmethod
    def _ensure_orders_column(connection: sqlite3.Connection, column_name: str, column_type: str) -> None:
        columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(orders)").fetchall()
        }
        if column_name in columns:
            return
        connection.execute(f"ALTER TABLE orders ADD COLUMN {column_name} {column_type}")

    def insert_book_event(self, snapshot: OrderBookSnapshot) -> int:
        bids_json = json.dumps([level.__dict__ for level in snapshot.bids])
        asks_json = json.dumps([level.__dict__ for level in snapshot.asks])
        raw_message_json = json.dumps(snapshot.raw_message) if snapshot.raw_message is not None else None
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO book_events (
                    market_id, asset_id, outcome_side, timestamp, sequence_id, is_reset,
                    best_bid, best_ask, mid_price, bids_json, asks_json, raw_message_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.market_id,
                    snapshot.asset_id,
                    snapshot.outcome_side,
                    snapshot.timestamp.isoformat(),
                    snapshot.sequence_id,
                    1 if snapshot.is_reset else 0,
                    snapshot.best_bid,
                    snapshot.best_ask,
                    snapshot.mid_price,
                    bids_json,
                    asks_json,
                    raw_message_json,
                ),
            )
            return int(cursor.lastrowid)

    def insert_whale_signal(self, signal: WhaleSignal) -> int:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO whale_signals (
                    market_id, asset_id, side, confidence, trigger_type, timestamp, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.market_id,
                    signal.asset_id,
                    signal.side,
                    signal.confidence,
                    signal.trigger_type,
                    signal.timestamp.isoformat(),
                    json.dumps(signal.details),
                ),
            )
            return int(cursor.lastrowid)

    def insert_order_intent(self, order: OrderIntent, status: str, details: dict | None = None) -> int:
        payload = details or {}
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO orders (
                    market_id, asset_id, side, size_usdh, quantity, price, client_order_id,
                    order_id, status, filled_price, paper_trade, signal_id,
                    created_at, updated_at, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, ?, ?, ?, ?, ?)
                """,
                (
                    order.market_id,
                    order.asset_id,
                    order.side,
                    order.size_usdh,
                    order.quantity,
                    order.price,
                    order.client_order_id,
                    status,
                    1 if order.paper_trade else 0,
                    order.signal_id,
                    order.timestamp.isoformat(),
                    order.timestamp.isoformat(),
                    json.dumps(payload),
                ),
            )
            return int(cursor.lastrowid)

    def update_order_result(self, client_order_id: str, result: OrderResult) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE orders
                SET order_id = ?, status = ?, filled_price = ?, updated_at = ?, details_json = ?
                WHERE client_order_id = ?
                """,
                (
                    result.order_id,
                    result.status,
                    result.filled_price,
                    result.timestamp.isoformat(),
                    json.dumps(result.details),
                    client_order_id,
                ),
            )

    def open_market_ids(self) -> set[str]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT DISTINCT market_id
                FROM orders
                WHERE status IN ('pending', 'submitted', 'paper_filled', 'filled')
                """
            ).fetchall()
            return {str(row["market_id"]) for row in rows}

    def total_reserved_exposure(self) -> float:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT COALESCE(SUM(size_usdh), 0.0) AS exposure
                FROM orders
                WHERE status IN ('pending', 'submitted', 'paper_filled', 'filled')
                """
            ).fetchone()
            return float(row["exposure"])

    def get_ledger_state(self) -> sqlite3.Row:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT cash_balance, equity, peak_equity, updated_at FROM ledger_state WHERE id = 1"
            ).fetchone()
            if row is None:
                raise RuntimeError("Ledger state is not initialized.")
            return row

    def update_ledger_state(self, cash_balance: float, equity: float, peak_equity: float | None = None) -> None:
        with self.connect() as connection:
            current = connection.execute(
                "SELECT peak_equity FROM ledger_state WHERE id = 1"
            ).fetchone()
            peak_value = float(current["peak_equity"]) if current is not None else 0.0
            next_peak = max(peak_value, equity) if peak_equity is None else peak_equity
            connection.execute(
                """
                UPDATE ledger_state
                SET cash_balance = ?, equity = ?, peak_equity = ?, updated_at = ?
                WHERE id = 1
                """,
                (cash_balance, equity, next_peak, datetime.utcnow().isoformat()),
            )
