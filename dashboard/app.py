from __future__ import annotations

import json
import sys
from pathlib import Path

from flask import Flask, jsonify, render_template

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import Config
from modules.storage import Storage

app = Flask(__name__, template_folder=str(ROOT / "dashboard" / "templates"))


def _normalize_unix_timestamp(timestamp: int | None) -> int | None:
    if timestamp is None:
        return None
    if timestamp > 10_000_000_000:
        return timestamp // 1000
    return timestamp


def get_storage() -> Storage:
    config = Config.from_env()
    storage = Storage(config.db_path)
    storage.initialize()
    return storage


@app.get("/")
def index() -> str:
    return render_template("index.html")


@app.get("/api/wallets")
def wallet_scores() -> object:
    rows = get_storage().list_wallet_scores()
    return jsonify(
        [
            {
                "address": row["address"],
                "trade_count": row["trade_count"],
                "win_count": row["win_count"],
                "win_rate": row["win_rate"],
                "total_pnl_usdh": row["total_pnl_usdh"],
                "avg_pnl_per_trade": row["avg_pnl_per_trade"],
                "last_trade_ts": _normalize_unix_timestamp(
                    int(row["last_trade_ts"]) if row["last_trade_ts"] is not None else None
                ),
                "last_updated_ts": row["last_updated_ts"],
            }
            for row in rows
            if int(row["trade_count"]) > 0
        ]
    )


@app.get("/api/signals")
def signals() -> object:
    rows = get_storage().list_recent_whale_signals(limit=50)
    return jsonify(
        [
            {
                "id": row["id"],
                "market_id": row["market_id"],
                "asset_id": row["asset_id"],
                "side": row["side"],
                "confidence": row["confidence"],
                "trigger_type": row["trigger_type"],
                "timestamp": row["timestamp"],
                "wallet_bonus_applied": bool(row["wallet_bonus_applied"]),
                "details": json.loads(row["details_json"]),
            }
            for row in rows
        ]
    )


@app.get("/api/orders")
def orders() -> object:
    rows = get_storage().list_recent_orders(limit=50)
    return jsonify(
        [
            {
                "id": row["id"],
                "market_id": row["market_id"],
                "asset_id": row["asset_id"],
                "side": row["side"],
                "size_usdh": row["size_usdh"],
                "quantity": row["quantity"],
                "price": row["price"],
                "client_order_id": row["client_order_id"],
                "order_id": row["order_id"],
                "status": row["status"],
                "filled_price": row["filled_price"],
                "paper_trade": bool(row["paper_trade"]),
                "signal_id": row["signal_id"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "details": json.loads(row["details_json"]),
            }
            for row in rows
        ]
    )


@app.get("/api/perp-signals")
def perp_signals() -> object:
    rows = get_storage().list_recent_perp_signals(limit=50)
    return jsonify(
        [
            {
                "id": row["id"],
                "market_id": row["market_id"],
                "asset_id": row["asset_id"],
                "coin": row["coin"],
                "side": row["side"],
                "confidence": row["confidence"],
                "trigger_type": row["trigger_type"],
                "timestamp": row["timestamp"],
                "wallet_bonus_applied": bool(row["wallet_bonus_applied"]),
                "trigger_oi_spike": bool(row["trigger_oi_spike"]),
                "trigger_funding": bool(row["trigger_funding"]),
                "details": json.loads(row["details_json"]),
            }
            for row in rows
        ]
    )


@app.get("/api/perp-orders")
def perp_orders() -> object:
    rows = get_storage().list_recent_perp_orders(limit=50)
    return jsonify(
        [
            {
                "id": row["id"],
                "market_id": row["market_id"],
                "asset_id": row["asset_id"],
                "side": row["side"],
                "size_usdh": row["size_usdh"],
                "quantity": row["quantity"],
                "price": row["price"],
                "client_order_id": row["client_order_id"],
                "order_id": row["order_id"],
                "status": row["status"],
                "filled_price": row["filled_price"],
                "paper_trade": bool(row["paper_trade"]),
                "signal_id": row["signal_id"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "details": json.loads(row["details_json"]),
            }
            for row in rows
        ]
    )


if __name__ == "__main__":
    app.run(debug=True)
