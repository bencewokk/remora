from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator

from .models import OrderBookSnapshot, OrderIntent, OrderResult, PerpSnapshot, PerpWhaleSignal, WalletScore, WhaleSignal


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
                    trigger_address TEXT,
                    best_bid REAL,
                    best_ask REAL,
                    mid_price REAL,
                    bids_json TEXT NOT NULL,
                    asks_json TEXT NOT NULL,
                    raw_message_json TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_book_events_market_time
                ON book_events (market_id, asset_id, timestamp);

                CREATE TABLE IF NOT EXISTS perp_book_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_id TEXT NOT NULL,
                    asset_id INTEGER NOT NULL,
                    coin TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    sequence_id INTEGER,
                    is_reset INTEGER NOT NULL DEFAULT 0,
                    trigger_address TEXT,
                    best_bid REAL,
                    best_ask REAL,
                    mid_price REAL,
                    funding_rate REAL NOT NULL DEFAULT 0,
                    open_interest REAL NOT NULL DEFAULT 0,
                    oi_change_pct REAL NOT NULL DEFAULT 0,
                    bids_json TEXT NOT NULL,
                    asks_json TEXT NOT NULL,
                    raw_message_json TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_perp_book_events_market_time
                ON perp_book_events (market_id, asset_id, timestamp);

                CREATE TABLE IF NOT EXISTS whale_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_id TEXT NOT NULL,
                    asset_id INTEGER NOT NULL,
                    side TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    trigger_type TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    wallet_bonus_applied INTEGER NOT NULL DEFAULT 0,
                    details_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_whale_signals_market_time
                ON whale_signals (market_id, timestamp);

                CREATE TABLE IF NOT EXISTS perp_whale_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_id TEXT NOT NULL,
                    asset_id INTEGER NOT NULL,
                    coin TEXT NOT NULL,
                    side TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    trigger_type TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    wallet_bonus_applied INTEGER NOT NULL DEFAULT 0,
                    trigger_oi_spike INTEGER NOT NULL DEFAULT 0,
                    trigger_funding INTEGER NOT NULL DEFAULT 0,
                    details_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_perp_whale_signals_market_time
                ON perp_whale_signals (market_id, timestamp);

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

                CREATE TABLE IF NOT EXISTS perp_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_id TEXT NOT NULL,
                    asset_id INTEGER NOT NULL,
                    side TEXT NOT NULL,
                    size_usdh REAL NOT NULL,
                    quantity REAL NOT NULL,
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
                    FOREIGN KEY (signal_id) REFERENCES perp_whale_signals (id)
                );

                CREATE INDEX IF NOT EXISTS idx_perp_orders_market_status
                ON perp_orders (market_id, status);

                CREATE TABLE IF NOT EXISTS ledger_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    cash_balance REAL NOT NULL,
                    equity REAL NOT NULL,
                    peak_equity REAL NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS perp_ledger_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    cash_balance REAL NOT NULL,
                    equity REAL NOT NULL,
                    peak_equity REAL NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS wallet_scores (
                    address TEXT PRIMARY KEY,
                    trade_count INTEGER NOT NULL,
                    win_count INTEGER NOT NULL,
                    win_rate REAL NOT NULL,
                    total_pnl_usdh REAL NOT NULL,
                    avg_pnl_per_trade REAL NOT NULL,
                    last_trade_ts INTEGER,
                    last_updated_ts INTEGER NOT NULL
                );
                """
            )
            self._ensure_orders_column(connection, "quantity", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_book_events_column(connection, "trigger_address", "TEXT")
            self._ensure_whale_signals_column(connection, "wallet_bonus_applied", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_perp_orders_column(connection, "quantity", "REAL NOT NULL DEFAULT 0")
            self._initialize_ledger_row(connection, "ledger_state")
            self._initialize_ledger_row(connection, "perp_ledger_state")

    @staticmethod
    def _initialize_ledger_row(connection: sqlite3.Connection, table_name: str) -> None:
        row = connection.execute(f"SELECT id FROM {table_name} WHERE id = 1").fetchone()
        if row is not None:
            return
        now = datetime.utcnow().isoformat()
        connection.execute(
            f"""
            INSERT INTO {table_name} (id, cash_balance, equity, peak_equity, updated_at)
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

    @staticmethod
    def _ensure_book_events_column(connection: sqlite3.Connection, column_name: str, column_type: str) -> None:
        columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(book_events)").fetchall()
        }
        if column_name in columns:
            return
        connection.execute(f"ALTER TABLE book_events ADD COLUMN {column_name} {column_type}")

    @staticmethod
    def _ensure_whale_signals_column(connection: sqlite3.Connection, column_name: str, column_type: str) -> None:
        columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(whale_signals)").fetchall()
        }
        if column_name in columns:
            return
        connection.execute(f"ALTER TABLE whale_signals ADD COLUMN {column_name} {column_type}")

    @staticmethod
    def _ensure_perp_orders_column(connection: sqlite3.Connection, column_name: str, column_type: str) -> None:
        columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(perp_orders)").fetchall()
        }
        if column_name in columns:
            return
        connection.execute(f"ALTER TABLE perp_orders ADD COLUMN {column_name} {column_type}")

    def insert_book_event(self, snapshot: OrderBookSnapshot) -> int:
        bids_json = json.dumps([level.__dict__ for level in snapshot.bids])
        asks_json = json.dumps([level.__dict__ for level in snapshot.asks])
        raw_message_json = json.dumps(snapshot.raw_message) if snapshot.raw_message is not None else None
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO book_events (
                    market_id, asset_id, outcome_side, timestamp, sequence_id, is_reset,
                    trigger_address,
                    best_bid, best_ask, mid_price, bids_json, asks_json, raw_message_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.market_id,
                    snapshot.asset_id,
                    snapshot.outcome_side,
                    snapshot.timestamp.isoformat(),
                    snapshot.sequence_id,
                    1 if snapshot.is_reset else 0,
                    snapshot.trigger_address,
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
                    market_id, asset_id, side, confidence, trigger_type, timestamp, wallet_bonus_applied, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.market_id,
                    signal.asset_id,
                    signal.side,
                    signal.confidence,
                    signal.trigger_type,
                    signal.timestamp.isoformat(),
                    1 if signal.wallet_bonus_applied else 0,
                    json.dumps(signal.details),
                ),
            )
            return int(cursor.lastrowid)

    def insert_perp_book_event(self, snapshot: PerpSnapshot) -> int:
        bids_json = json.dumps([level.__dict__ for level in snapshot.bids])
        asks_json = json.dumps([level.__dict__ for level in snapshot.asks])
        raw_message_json = json.dumps(snapshot.raw_message) if snapshot.raw_message is not None else None
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO perp_book_events (
                    market_id, asset_id, coin, timestamp, sequence_id, is_reset,
                    trigger_address, best_bid, best_ask, mid_price,
                    funding_rate, open_interest, oi_change_pct,
                    bids_json, asks_json, raw_message_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.market_id,
                    snapshot.asset_id,
                    snapshot.coin,
                    snapshot.timestamp.isoformat(),
                    snapshot.sequence_id,
                    1 if snapshot.is_reset else 0,
                    snapshot.trigger_address,
                    snapshot.best_bid,
                    snapshot.best_ask,
                    snapshot.mid_price,
                    snapshot.funding_rate,
                    snapshot.open_interest,
                    snapshot.oi_change_pct,
                    bids_json,
                    asks_json,
                    raw_message_json,
                ),
            )
            return int(cursor.lastrowid)

    def insert_perp_whale_signal(self, signal: PerpWhaleSignal) -> int:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO perp_whale_signals (
                    market_id, asset_id, coin, side, confidence, trigger_type, timestamp,
                    wallet_bonus_applied, trigger_oi_spike, trigger_funding, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.market_id,
                    signal.asset_id,
                    signal.coin,
                    signal.side,
                    signal.confidence,
                    signal.trigger_type,
                    signal.timestamp.isoformat(),
                    1 if signal.wallet_bonus_applied else 0,
                    1 if signal.trigger_oi_spike else 0,
                    1 if signal.trigger_funding else 0,
                    json.dumps(signal.details),
                ),
            )
            return int(cursor.lastrowid)

    def get_wallet_score(self, address: str) -> WalletScore | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT address, trade_count, win_count, win_rate, total_pnl_usdh,
                       avg_pnl_per_trade, last_trade_ts, last_updated_ts
                FROM wallet_scores
                WHERE lower(address) = lower(?)
                """,
                (address,),
            ).fetchone()
            if row is None:
                return None
            return WalletScore(
                address=str(row["address"]),
                trade_count=int(row["trade_count"]),
                win_count=int(row["win_count"]),
                win_rate=float(row["win_rate"]),
                total_pnl_usdh=float(row["total_pnl_usdh"]),
                avg_pnl_per_trade=float(row["avg_pnl_per_trade"]),
                last_trade_ts=int(row["last_trade_ts"]) if row["last_trade_ts"] is not None else None,
                last_updated_ts=int(row["last_updated_ts"]),
            )

    def upsert_wallet_scores(self, scores: list[WalletScore]) -> None:
        if not scores:
            return
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO wallet_scores (
                    address, trade_count, win_count, win_rate, total_pnl_usdh,
                    avg_pnl_per_trade, last_trade_ts, last_updated_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(address) DO UPDATE SET
                    trade_count = excluded.trade_count,
                    win_count = excluded.win_count,
                    win_rate = excluded.win_rate,
                    total_pnl_usdh = excluded.total_pnl_usdh,
                    avg_pnl_per_trade = excluded.avg_pnl_per_trade,
                    last_trade_ts = excluded.last_trade_ts,
                    last_updated_ts = excluded.last_updated_ts
                """,
                [
                    (
                        score.address,
                        score.trade_count,
                        score.win_count,
                        score.win_rate,
                        score.total_pnl_usdh,
                        score.avg_pnl_per_trade,
                        score.last_trade_ts,
                        score.last_updated_ts,
                    )
                    for score in scores
                ],
            )

    def list_tracked_wallet_addresses(self) -> list[str]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT DISTINCT trigger_address AS address
                FROM book_events
                WHERE trigger_address IS NOT NULL AND trigger_address != ''
                UNION
                SELECT DISTINCT trigger_address AS address
                FROM perp_book_events
                WHERE trigger_address IS NOT NULL AND trigger_address != ''
                UNION
                SELECT address
                FROM wallet_scores
                """
            ).fetchall()
            return [str(row["address"]) for row in rows]

    def list_initial_tracked_wallet_addresses(self, limit: int = 5) -> list[str]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT address
                FROM (
                    SELECT trigger_address AS address, MIN(timestamp) AS first_seen_at
                    FROM book_events
                    WHERE trigger_address IS NOT NULL AND trigger_address != ''
                    GROUP BY trigger_address

                    UNION ALL

                    SELECT trigger_address AS address, MIN(timestamp) AS first_seen_at
                    FROM perp_book_events
                    WHERE trigger_address IS NOT NULL AND trigger_address != ''
                    GROUP BY trigger_address
                ) AS first_seen
                GROUP BY address
                ORDER BY MIN(first_seen_at) ASC, LOWER(address) ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [str(row["address"]) for row in rows]

    def list_wallet_scores(self) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT address, trade_count, win_count, win_rate, total_pnl_usdh,
                       avg_pnl_per_trade, last_trade_ts, last_updated_ts
                FROM wallet_scores
                ORDER BY win_rate DESC, trade_count DESC, total_pnl_usdh DESC
                """
            ).fetchall()

    def list_recent_whale_signals(self, limit: int = 50) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT id, market_id, asset_id, side, confidence, trigger_type,
                       timestamp, wallet_bonus_applied, details_json
                FROM whale_signals
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    def list_recent_perp_signals(self, limit: int = 50) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT id, market_id, asset_id, coin, side, confidence, trigger_type,
                       timestamp, wallet_bonus_applied, trigger_oi_spike, trigger_funding, details_json
                FROM perp_whale_signals
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    def list_recent_orders(self, limit: int = 50) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT id, market_id, asset_id, side, size_usdh, quantity, price,
                       client_order_id, order_id, status, filled_price, paper_trade,
                       signal_id, created_at, updated_at, details_json
                FROM orders
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    def list_open_orders(self) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT orders.id, orders.market_id, orders.asset_id, orders.side, orders.size_usdh,
                       orders.quantity, orders.price, orders.client_order_id, orders.order_id,
                       orders.status, orders.filled_price, orders.paper_trade, orders.signal_id,
                       orders.created_at, orders.updated_at, orders.details_json,
                       whale_signals.side AS signal_side
                FROM orders
                LEFT JOIN whale_signals ON whale_signals.id = orders.signal_id
                WHERE status IN ('pending', 'submitted', 'paper_filled', 'filled')
                ORDER BY created_at DESC
                """
            ).fetchall()

    def list_recent_perp_orders(self, limit: int = 50) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT id, market_id, asset_id, side, size_usdh, quantity, price,
                       client_order_id, order_id, status, filled_price, paper_trade,
                       signal_id, created_at, updated_at, details_json
                FROM perp_orders
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    def list_open_perp_orders(self) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT perp_orders.id, perp_orders.market_id, perp_orders.asset_id, perp_orders.side,
                       perp_orders.size_usdh, perp_orders.quantity, perp_orders.price,
                       perp_orders.client_order_id, perp_orders.order_id, perp_orders.status,
                       perp_orders.filled_price, perp_orders.paper_trade, perp_orders.signal_id,
                       perp_orders.created_at, perp_orders.updated_at, perp_orders.details_json,
                       perp_whale_signals.side AS signal_side, perp_whale_signals.coin AS signal_coin
                FROM perp_orders
                LEFT JOIN perp_whale_signals ON perp_whale_signals.id = perp_orders.signal_id
                WHERE status IN ('pending', 'submitted', 'paper_filled', 'filled')
                ORDER BY created_at DESC
                """
            ).fetchall()

    def latest_prediction_marks(self) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT current.market_id, current.asset_id, current.timestamp,
                       current.best_bid, current.best_ask, current.mid_price
                FROM book_events AS current
                INNER JOIN (
                    SELECT market_id, asset_id, MAX(timestamp) AS max_timestamp
                    FROM book_events
                    GROUP BY market_id, asset_id
                ) AS latest
                  ON latest.market_id = current.market_id
                 AND latest.asset_id = current.asset_id
                 AND latest.max_timestamp = current.timestamp
                """
            ).fetchall()

    def latest_perp_marks(self) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT current.market_id, current.asset_id, current.coin, current.timestamp,
                       current.best_bid, current.best_ask, current.mid_price,
                       current.funding_rate, current.open_interest
                FROM perp_book_events AS current
                INNER JOIN (
                    SELECT market_id, asset_id, MAX(timestamp) AS max_timestamp
                    FROM perp_book_events
                    GROUP BY market_id, asset_id
                ) AS latest
                  ON latest.market_id = current.market_id
                 AND latest.asset_id = current.asset_id
                 AND latest.max_timestamp = current.timestamp
                """
            ).fetchall()

    def latest_perp_mark(self, market_id: str, asset_id: int) -> sqlite3.Row | None:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT market_id, asset_id, coin, timestamp,
                       best_bid, best_ask, mid_price,
                       funding_rate, open_interest
                FROM perp_book_events
                WHERE market_id = ? AND asset_id = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (market_id, asset_id),
            ).fetchone()

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

    def insert_perp_order_intent(self, order: OrderIntent, status: str, details: dict | None = None) -> int:
        payload = details or {}
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO perp_orders (
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

    def update_perp_order_result(self, client_order_id: str, result: OrderResult) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE perp_orders
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

    def open_perp_market_ids(self) -> set[str]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT DISTINCT market_id
                FROM perp_orders
                WHERE status IN ('pending', 'submitted', 'paper_filled', 'filled')
                """
            ).fetchall()
            return {str(row["market_id"]) for row in rows}

    def total_reserved_perp_exposure(self) -> float:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT COALESCE(SUM(size_usdh), 0.0) AS exposure
                FROM perp_orders
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

    def get_perp_ledger_state(self) -> sqlite3.Row:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT cash_balance, equity, peak_equity, updated_at FROM perp_ledger_state WHERE id = 1"
            ).fetchone()
            if row is None:
                raise RuntimeError("Perp ledger state is not initialized.")
            return row

    def update_perp_ledger_state(self, cash_balance: float, equity: float, peak_equity: float | None = None) -> None:
        with self.connect() as connection:
            current = connection.execute(
                "SELECT peak_equity FROM perp_ledger_state WHERE id = 1"
            ).fetchone()
            peak_value = float(current["peak_equity"]) if current is not None else 0.0
            next_peak = max(peak_value, equity) if peak_equity is None else peak_equity
            connection.execute(
                """
                UPDATE perp_ledger_state
                SET cash_balance = ?, equity = ?, peak_equity = ?, updated_at = ?
                WHERE id = 1
                """,
                (cash_balance, equity, next_peak, datetime.utcnow().isoformat()),
            )
