from __future__ import annotations

from datetime import datetime, timezone
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

SIGNAL_MARKOUT_LIMIT = 250


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


def _parse_iso_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _latest_prediction_marks(storage: Storage) -> dict[tuple[str, int], dict]:
    return {
        (str(row["market_id"]), int(row["asset_id"])): {
            "mid_price": float(row["mid_price"]) if row["mid_price"] is not None else None,
            "timestamp": row["timestamp"],
        }
        for row in storage.latest_prediction_marks()
    }


def _latest_perp_marks(storage: Storage) -> dict[tuple[str, int], dict]:
    return {
        (str(row["market_id"]), int(row["asset_id"])): {
            "coin": row["coin"],
            "mid_price": float(row["mid_price"]) if row["mid_price"] is not None else None,
            "timestamp": row["timestamp"],
            "funding_rate": float(row["funding_rate"]),
            "open_interest": float(row["open_interest"]),
        }
        for row in storage.latest_perp_marks()
    }


def _signal_markout_summary(
    rows: list,
    latest_marks: dict,
    side_map: dict[str, int],
) -> dict[str, float | int | None]:
    evaluated = 0
    winners = 0
    total_markout_pct = 0.0
    total_confidence = 0.0
    for row in rows:
        details = json.loads(row["details_json"])
        entry_mid = details.get("mid_price")
        if not isinstance(entry_mid, (int, float)) or float(entry_mid) <= 0:
            continue
        latest_mark = latest_marks.get((str(row["market_id"]), int(row["asset_id"])))
        if latest_mark is None:
            continue
        current_mid = latest_mark.get("mid_price")
        if not isinstance(current_mid, (int, float)):
            continue
        direction = side_map.get(str(row["side"]))
        if direction is None:
            continue
        evaluated += 1
        total_confidence += float(row["confidence"])
        markout_pct = direction * ((float(current_mid) - float(entry_mid)) / float(entry_mid))
        total_markout_pct += markout_pct
        if markout_pct > 0:
            winners += 1

    return {
        "evaluated_signals": evaluated,
        "winning_signals": winners,
        "losing_signals": max(evaluated - winners, 0),
        "win_rate": (winners / evaluated) if evaluated else None,
        "avg_markout_pct": (total_markout_pct / evaluated) if evaluated else None,
        "avg_confidence": (total_confidence / evaluated) if evaluated else None,
    }


def _prediction_positions(storage: Storage, latest_marks: dict[tuple[str, int], dict]) -> list[dict]:
    positions: list[dict] = []
    for row in storage.list_open_orders():
        entry_price = float(row["filled_price"] if row["filled_price"] is not None else row["price"])
        quantity = float(row["quantity"])
        latest_mark = latest_marks.get((str(row["market_id"]), int(row["asset_id"])))
        current_mark = latest_mark.get("mid_price") if latest_mark is not None else None
        unrealized_pnl = None
        if isinstance(current_mark, (int, float)):
            unrealized_pnl = quantity * (float(current_mark) - entry_price)
        created_at = _parse_iso_timestamp(str(row["created_at"]))
        age_seconds = None
        if created_at is not None:
            age_seconds = max(int((datetime.now(tz=timezone.utc) - created_at).total_seconds()), 0)
        positions.append(
            {
                "market_type": "prediction",
                "market_id": row["market_id"],
                "asset_id": row["asset_id"],
                "contract_side": row["signal_side"] or row["side"],
                "status": row["status"],
                "entry_price": entry_price,
                "current_mark_price": current_mark,
                "quantity": quantity,
                "size_usdh": float(row["size_usdh"]),
                "unrealized_pnl_usdh": unrealized_pnl,
                "created_at": row["created_at"],
                "age_seconds": age_seconds,
                "mark_timestamp": latest_mark.get("timestamp") if latest_mark is not None else None,
            }
        )
    return positions


def _perp_positions(storage: Storage, latest_marks: dict[tuple[str, int], dict]) -> list[dict]:
    positions: list[dict] = []
    for row in storage.list_open_perp_orders():
        entry_price = float(row["filled_price"] if row["filled_price"] is not None else row["price"])
        quantity = float(row["quantity"])
        latest_mark = latest_marks.get((str(row["market_id"]), int(row["asset_id"])))
        current_mark = latest_mark.get("mid_price") if latest_mark is not None else None
        direction = 1.0 if str(row["side"]).lower() == "buy" else -1.0
        unrealized_pnl = None
        if isinstance(current_mark, (int, float)):
            unrealized_pnl = quantity * (float(current_mark) - entry_price) * direction
        created_at = _parse_iso_timestamp(str(row["created_at"]))
        age_seconds = None
        if created_at is not None:
            age_seconds = max(int((datetime.now(tz=timezone.utc) - created_at).total_seconds()), 0)
        positions.append(
            {
                "market_type": "perp",
                "market_id": row["market_id"],
                "coin": row["signal_coin"] or row["market_id"],
                "direction": row["signal_side"] or row["side"],
                "status": row["status"],
                "entry_price": entry_price,
                "current_mark_price": current_mark,
                "quantity": quantity,
                "size_usdh": float(row["size_usdh"]),
                "unrealized_pnl_usdh": unrealized_pnl,
                "created_at": row["created_at"],
                "age_seconds": age_seconds,
                "mark_timestamp": latest_mark.get("timestamp") if latest_mark is not None else None,
                "funding_rate": latest_mark.get("funding_rate") if latest_mark is not None else None,
                "open_interest": latest_mark.get("open_interest") if latest_mark is not None else None,
            }
        )
    return positions


def _portfolio_summary(prediction_positions: list[dict], perp_positions: list[dict], storage: Storage) -> dict[str, float | int]:
    prediction_unrealized = sum(position["unrealized_pnl_usdh"] or 0.0 for position in prediction_positions)
    perp_unrealized = sum(position["unrealized_pnl_usdh"] or 0.0 for position in perp_positions)
    return {
        "prediction_open_positions": len(prediction_positions),
        "perp_open_positions": len(perp_positions),
        "total_open_positions": len(prediction_positions) + len(perp_positions),
        "prediction_reserved_exposure_usdh": storage.total_reserved_exposure(),
        "perp_reserved_exposure_usdh": storage.total_reserved_perp_exposure(),
        "prediction_unrealized_pnl_usdh": prediction_unrealized,
        "perp_unrealized_pnl_usdh": perp_unrealized,
        "total_unrealized_pnl_usdh": prediction_unrealized + perp_unrealized,
    }


@app.get("/")
def index() -> str:
    return render_template("index.html")


@app.get("/api/dashboard-summary")
def dashboard_summary() -> object:
    storage = get_storage()
    prediction_marks = _latest_prediction_marks(storage)
    perp_marks = _latest_perp_marks(storage)
    prediction_positions = _prediction_positions(storage, prediction_marks)
    perp_positions = _perp_positions(storage, perp_marks)
    return jsonify(
        {
            "signal_quality": {
                "prediction": _signal_markout_summary(
                    storage.list_recent_whale_signals(limit=SIGNAL_MARKOUT_LIMIT),
                    prediction_marks,
                    {"YES": 1, "NO": -1},
                ),
                "perp": _signal_markout_summary(
                    storage.list_recent_perp_signals(limit=SIGNAL_MARKOUT_LIMIT),
                    perp_marks,
                    {"long": 1, "short": -1},
                ),
            },
            "portfolio": _portfolio_summary(prediction_positions, perp_positions, storage),
        }
    )


@app.get("/api/positions")
def positions() -> object:
    storage = get_storage()
    prediction_marks = _latest_prediction_marks(storage)
    perp_marks = _latest_perp_marks(storage)
    prediction_positions = _prediction_positions(storage, prediction_marks)
    perp_positions = _perp_positions(storage, perp_marks)
    return jsonify(
        {
            "prediction": prediction_positions,
            "perp": perp_positions,
            "all": prediction_positions + perp_positions,
        }
    )


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
