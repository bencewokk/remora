from __future__ import annotations

import asyncio
from bisect import bisect_right
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
import json
import sys
import time
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template, request

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import Config
from modules import HyperliquidAdapter, discover_active_markets
from modules.storage import Storage

app = Flask(__name__, template_folder=str(ROOT / "dashboard" / "templates"))

SIGNAL_MARKOUT_LIMIT = 250
SIGNAL_QUALITY_WINDOW_SECONDS = 300
MAX_PNL_SERIES_POINTS = 120
DECISION_QUALITY_TOP_PERP_COINS = 8
PREDICTION_MARKET_META_TTL_SECONDS = 300
WHALE_SPOTLIGHT_CACHE_TTL_SECONDS = 300
CLUSTER_WINDOW_SECONDS = 30
CLUSTER_WINDOW_MIN_SECONDS = 10
CLUSTER_WINDOW_MAX_SECONDS = 120
CLUSTER_COLLAPSE_SECONDS = 5
CLUSTER_MIN_EDGE_WEIGHT = 2
CLUSTER_CONVOY_MIN_WIN_RATE = 0.60
CLUSTER_CONVOY_MIN_TRADE_COUNT = 5
CLUSTER_RECENT_TRADES_LIMIT = 5
SIGNAL_QUALITY_TRIGGER_TYPES = (
    "single_step_liquidity",
    "rapid_sequential_aggression",
    "price_impact_with_aggression",
    "oi_spike",
    "funding_divergence",
)
OPEN_ORDER_STATUSES = ("pending", "submitted", "paper_filled", "filled")
CLOSED_ORDER_STATUSES = ("paper_settled", "settled")
_PREDICTION_MARKET_META_CACHE: dict[str, Any] = {"loaded_at": 0.0, "data": {}}
_WHALE_SPOTLIGHT_CACHE: dict[str, Any] = {"loaded_at": 0.0, "addresses": tuple(), "data": {}}


def _normalize_unix_timestamp(timestamp: int | None) -> int | None:
    if timestamp is None:
        return None
    if timestamp > 10_000_000_000:
        return timestamp // 1000
    return timestamp


def _normalize_trigger_type(trigger_type: str) -> str:
    if trigger_type == "price_impact":
        return "price_impact_with_aggression"
    return trigger_type


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


def _parse_expiry_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed = datetime.fromisoformat(f"{normalized}T00:00:00+00:00")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _json_object(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        payload = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _hours_between(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return round(max((end - start).total_seconds(), 0.0) / 3600.0, 1)


def _positive_peak(values: list[float]) -> float:
    positive_values = [value for value in values if value > 0]
    if not positive_values:
        return 0.0
    return min(positive_values)


def _ledger_starting_cash(storage: Storage) -> float:
    prediction_ledger = storage.get_ledger_state()
    perp_ledger = storage.get_perp_ledger_state()
    peak_baseline = _positive_peak(
        [
            float(prediction_ledger["peak_equity"] or 0.0),
            float(perp_ledger["peak_equity"] or 0.0),
        ]
    )
    if peak_baseline > 0:
        return peak_baseline

    configured_starting_cash = max(float(Config.from_env().starting_cash_usdh), 0.0)
    if configured_starting_cash > 0:
        return configured_starting_cash

    return _positive_peak(
        [
            float(prediction_ledger["equity"] or 0.0),
            float(perp_ledger["equity"] or 0.0),
            float(prediction_ledger["cash_balance"] or 0.0),
            float(perp_ledger["cash_balance"] or 0.0),
        ]
    )


async def _load_prediction_market_meta() -> dict[str, dict[str, Any]]:
    config = Config.from_env()
    adapter = HyperliquidAdapter(config)
    try:
        markets = await discover_active_markets(adapter)
    finally:
        await adapter.close()
    return {
        market.market_id: {
            "title": market.title,
            "underlying": market.underlying,
            "expiry": market.expiry,
        }
        for market in markets
    }


def _prediction_market_meta() -> dict[str, dict[str, Any]]:
    now = time.time()
    cached_data = _PREDICTION_MARKET_META_CACHE.get("data", {})
    if now - float(_PREDICTION_MARKET_META_CACHE.get("loaded_at", 0.0)) < PREDICTION_MARKET_META_TTL_SECONDS and cached_data:
        return cached_data
    try:
        data = asyncio.run(_load_prediction_market_meta())
    except Exception:
        return cached_data
    _PREDICTION_MARKET_META_CACHE["loaded_at"] = now
    _PREDICTION_MARKET_META_CACHE["data"] = data
    return data


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


def _fetch_signal_rows(storage: Storage, table_name: str) -> list[Any]:
    with storage.connect() as connection:
        return connection.execute(
            f"""
            SELECT id, market_id, asset_id, side, confidence, trigger_type, timestamp, details_json
            FROM {table_name}
            ORDER BY timestamp DESC
            """
        ).fetchall()


def _fetch_prediction_decision_signal_rows(storage: Storage) -> list[Any]:
    with storage.connect() as connection:
        return connection.execute(
            """
            SELECT id, market_id, asset_id, side, confidence, trigger_type, timestamp,
                   wallet_bonus_applied, details_json
            FROM whale_signals
            ORDER BY timestamp DESC
            """
        ).fetchall()


def _fetch_perp_decision_signal_rows(storage: Storage) -> list[Any]:
    with storage.connect() as connection:
        return connection.execute(
            """
            SELECT id, market_id, asset_id, coin, side, confidence, trigger_type, timestamp,
                   wallet_bonus_applied, details_json
            FROM perp_whale_signals
            ORDER BY timestamp DESC
            """
        ).fetchall()


def _followed_signal_ids(storage: Storage, table_name: str) -> set[int]:
    with storage.connect() as connection:
        rows = connection.execute(
            f"""
            SELECT DISTINCT signal_id
            FROM {table_name}
            WHERE signal_id IS NOT NULL
            """
        ).fetchall()
    return {int(row["signal_id"]) for row in rows if row["signal_id"] is not None}


def _fetch_mark_rows(storage: Storage, table_name: str, keys: set[tuple[str, int]]) -> list[dict[str, Any]]:
    if not keys:
        return []
    clauses = " OR ".join("(market_id = ? AND asset_id = ?)" for _ in keys)
    params: list[Any] = []
    for market_id, asset_id in keys:
        params.extend((market_id, asset_id))
    with storage.connect() as connection:
        rows = connection.execute(
            f"""
            SELECT market_id, asset_id, timestamp, mid_price
            FROM {table_name}
            WHERE mid_price IS NOT NULL AND ({clauses})
            ORDER BY market_id, asset_id, timestamp
            """,
            params,
        ).fetchall()
    event_rows: list[dict[str, Any]] = []
    for row in rows:
        timestamp = _parse_iso_timestamp(str(row["timestamp"]))
        if timestamp is None:
            continue
        event_rows.append(
            {
                "key": (str(row["market_id"]), int(row["asset_id"])),
                "timestamp": timestamp,
                "timestamp_seconds": timestamp.timestamp(),
                "mid_price": float(row["mid_price"]),
            }
        )
    return event_rows


def _build_event_series(event_rows: list[dict[str, Any]]) -> dict[tuple[str, int], dict[str, list[float]]]:
    grouped: dict[tuple[str, int], dict[str, list[float]]] = defaultdict(lambda: {"times": [], "mid_prices": []})
    for row in event_rows:
        grouped[row["key"]]["times"].append(float(row["timestamp_seconds"]))
        grouped[row["key"]]["mid_prices"].append(float(row["mid_price"]))
    return dict(grouped)


def _markout_mid_within_window(
    event_series: dict[str, list[float]] | None,
    signal_timestamp_seconds: float,
    window_seconds: int,
) -> float | None:
    if not event_series:
        return None
    times = event_series["times"]
    mid_prices = event_series["mid_prices"]
    start_index = bisect_right(times, signal_timestamp_seconds)
    end_index = bisect_right(times, signal_timestamp_seconds + window_seconds)
    if start_index >= end_index:
        return None
    return float(mid_prices[end_index - 1])


def _empty_trigger_quality(trigger_type: str) -> dict[str, Any]:
    return {
        "trigger_type": trigger_type,
        "total_signals": 0,
        "evaluated_signals": 0,
        "favorable_signals": 0,
        "favorable_pct": None,
        "avg_markout_pct": None,
    }


def _signal_quality_breakdown(
    signal_rows: list[Any],
    event_series: dict[tuple[str, int], dict[str, list[float]]],
    side_map: dict[str, int],
) -> dict[str, dict[str, Any]]:
    stats = {trigger: _empty_trigger_quality(trigger) for trigger in SIGNAL_QUALITY_TRIGGER_TYPES}
    markout_sums = {trigger: 0.0 for trigger in SIGNAL_QUALITY_TRIGGER_TYPES}
    for row in signal_rows:
        signal_timestamp = _parse_iso_timestamp(str(row["timestamp"]))
        if signal_timestamp is None:
            continue
        details = json.loads(row["details_json"])
        entry_mid = details.get("mid_price")
        if not isinstance(entry_mid, (int, float)) or float(entry_mid) <= 0:
            markout_pct = None
        else:
            direction = side_map.get(str(row["side"]))
            future_mid = _markout_mid_within_window(
                event_series.get((str(row["market_id"]), int(row["asset_id"]))),
                signal_timestamp.timestamp(),
                SIGNAL_QUALITY_WINDOW_SECONDS,
            )
            if direction is None or future_mid is None:
                markout_pct = None
            else:
                markout_pct = direction * ((float(future_mid) - float(entry_mid)) / float(entry_mid))

        trigger_components = {
            _normalize_trigger_type(component)
            for component in str(row["trigger_type"]).split("+")
        }
        for trigger_type in trigger_components:
            if trigger_type not in stats:
                continue
            stats[trigger_type]["total_signals"] += 1
            if markout_pct is None:
                continue
            stats[trigger_type]["evaluated_signals"] += 1
            markout_sums[trigger_type] += markout_pct
            if markout_pct > 0:
                stats[trigger_type]["favorable_signals"] += 1

    for trigger_type, payload in stats.items():
        evaluated = int(payload["evaluated_signals"])
        if evaluated:
            payload["favorable_pct"] = float(payload["favorable_signals"]) / evaluated
            payload["avg_markout_pct"] = markout_sums[trigger_type] / evaluated
    return stats


def _signal_markout_evaluations(
    signal_rows: list[Any],
    event_series: dict[tuple[str, int], dict[str, list[float]]],
    side_map: dict[str, int],
    followed_signal_ids: set[int],
    market_type: str,
) -> list[dict[str, Any]]:
    evaluations: list[dict[str, Any]] = []
    for row in signal_rows:
        signal_timestamp = _parse_iso_timestamp(str(row["timestamp"]))
        if signal_timestamp is None:
            continue
        details = _json_object(row["details_json"])
        entry_mid = details.get("mid_price")
        direction = side_map.get(str(row["side"]))
        markout_pct = None
        if isinstance(entry_mid, (int, float)) and float(entry_mid) > 0 and direction is not None:
            future_mid = _markout_mid_within_window(
                event_series.get((str(row["market_id"]), int(row["asset_id"]))),
                signal_timestamp.timestamp(),
                SIGNAL_QUALITY_WINDOW_SECONDS,
            )
            if future_mid is not None:
                markout_pct = direction * ((float(future_mid) - float(entry_mid)) / float(entry_mid))
        try:
            coin = row["coin"]
        except (IndexError, KeyError):
            coin = None
        trigger_components = tuple(
            sorted(
                {
                    _normalize_trigger_type(component)
                    for component in str(row["trigger_type"]).split("+")
                    if component
                }
            )
        )
        evaluations.append(
            {
                "signal_id": int(row["id"]),
                "market_type": market_type,
                "market_id": str(row["market_id"]),
                "asset_id": int(row["asset_id"]),
                "coin": str(coin) if isinstance(coin, str) and coin else None,
                "side": str(row["side"]),
                "trigger_type": str(row["trigger_type"]),
                "trigger_components": trigger_components,
                "wallet_bonus_applied": bool(row["wallet_bonus_applied"]),
                "markout_pct": markout_pct,
                "followed": int(row["id"]) in followed_signal_ids,
            }
        )
    return evaluations


def _decision_quality_stats(evaluations: list[dict[str, Any]]) -> dict[str, Any]:
    evaluated = [evaluation for evaluation in evaluations if evaluation["markout_pct"] is not None]
    favorable = [evaluation for evaluation in evaluated if float(evaluation["markout_pct"]) > 0]
    avg_markout_pct = None
    if evaluated:
        avg_markout_pct = sum(float(evaluation["markout_pct"]) for evaluation in evaluated) / len(evaluated)
    return {
        "total_signals": len(evaluations),
        "evaluated_signals": len(evaluated),
        "favorable_signals": len(favorable),
        "favorable_pct": (len(favorable) / len(evaluated)) if evaluated else None,
        "avg_markout_pct": avg_markout_pct,
    }


def _decision_quality_comparison(evaluations: list[dict[str, Any]]) -> dict[str, Any]:
    followed = [evaluation for evaluation in evaluations if evaluation["followed"]]
    baseline_stats = _decision_quality_stats(evaluations)
    followed_stats = _decision_quality_stats(followed)
    markout_lift_pct = None
    favorable_lift_pct = None
    if baseline_stats["avg_markout_pct"] is not None and followed_stats["avg_markout_pct"] is not None:
        markout_lift_pct = float(followed_stats["avg_markout_pct"]) - float(baseline_stats["avg_markout_pct"])
    if baseline_stats["favorable_pct"] is not None and followed_stats["favorable_pct"] is not None:
        favorable_lift_pct = float(followed_stats["favorable_pct"]) - float(baseline_stats["favorable_pct"])
    return {
        "followed": followed_stats,
        "baseline": baseline_stats,
        "selection_rate": (len(followed) / len(evaluations)) if evaluations else None,
        "markout_lift_pct": markout_lift_pct,
        "favorable_lift_pct": favorable_lift_pct,
    }


def _decision_quality_breakdown_rows(
    evaluations: list[dict[str, Any]],
    group_values: list[Any],
    group_predicate: Any,
    label_key: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for value in group_values:
        group_evaluations = [evaluation for evaluation in evaluations if group_predicate(evaluation, value)]
        if not group_evaluations:
            continue
        rows.append(
            {
                label_key: value,
                "comparison": _decision_quality_comparison(group_evaluations),
            }
        )
    return rows


def _decision_quality_payload(storage: Storage) -> dict[str, Any]:
    prediction_signal_rows = _fetch_prediction_decision_signal_rows(storage)
    perp_signal_rows = _fetch_perp_decision_signal_rows(storage)
    prediction_event_rows = _fetch_mark_rows(
        storage,
        "book_events",
        {(str(row["market_id"]), int(row["asset_id"])) for row in prediction_signal_rows},
    )
    perp_event_rows = _fetch_mark_rows(
        storage,
        "perp_book_events",
        {(str(row["market_id"]), int(row["asset_id"])) for row in perp_signal_rows},
    )
    prediction_evaluations = _signal_markout_evaluations(
        prediction_signal_rows,
        _build_event_series(prediction_event_rows),
        {"YES": 1, "NO": -1},
        _followed_signal_ids(storage, "orders"),
        market_type="prediction",
    )
    perp_evaluations = _signal_markout_evaluations(
        perp_signal_rows,
        _build_event_series(perp_event_rows),
        {"long": 1, "short": -1},
        _followed_signal_ids(storage, "perp_orders"),
        market_type="perp",
    )
    perp_coin_values = [
        coin
        for coin, _count in sorted(
            defaultdict(int, {
                evaluation["coin"]: sum(1 for candidate in perp_evaluations if candidate["coin"] == evaluation["coin"])
                for evaluation in perp_evaluations
                if evaluation["coin"]
            }).items(),
            key=lambda item: (-item[1], item[0]),
        )[:DECISION_QUALITY_TOP_PERP_COINS]
    ]

    trigger_breakdown = []
    for trigger_type in SIGNAL_QUALITY_TRIGGER_TYPES:
        trigger_breakdown.append(
            {
                "trigger_type": trigger_type,
                "prediction": _decision_quality_comparison(
                    [evaluation for evaluation in prediction_evaluations if trigger_type in evaluation["trigger_components"]]
                ),
                "perp": _decision_quality_comparison(
                    [evaluation for evaluation in perp_evaluations if trigger_type in evaluation["trigger_components"]]
                ),
            }
        )

    wallet_bonus_breakdown = [
        {
            "wallet_bonus_applied": wallet_bonus_applied,
            "prediction": _decision_quality_comparison(
                [evaluation for evaluation in prediction_evaluations if evaluation["wallet_bonus_applied"] is wallet_bonus_applied]
            ),
            "perp": _decision_quality_comparison(
                [evaluation for evaluation in perp_evaluations if evaluation["wallet_bonus_applied"] is wallet_bonus_applied]
            ),
        }
        for wallet_bonus_applied in (False, True)
    ]

    return {
        "window_seconds": SIGNAL_QUALITY_WINDOW_SECONDS,
        "summary": {
            "prediction": _decision_quality_comparison(prediction_evaluations),
            "perp": _decision_quality_comparison(perp_evaluations),
        },
        "trigger_breakdown": trigger_breakdown,
        "wallet_bonus_breakdown": wallet_bonus_breakdown,
        "prediction_side_breakdown": _decision_quality_breakdown_rows(
            prediction_evaluations,
            ["YES", "NO"],
            lambda evaluation, side: evaluation["side"] == side,
            "side",
        ),
        "perp_side_breakdown": _decision_quality_breakdown_rows(
            perp_evaluations,
            ["long", "short"],
            lambda evaluation, side: evaluation["side"] == side,
            "side",
        ),
        "perp_coin_breakdown": _decision_quality_breakdown_rows(
            perp_evaluations,
            perp_coin_values,
            lambda evaluation, coin: evaluation["coin"] == coin,
            "coin",
        ),
    }


def _sample_timestamps(points: list[float], max_points: int) -> list[float]:
    unique_points = sorted(set(points))
    if len(unique_points) <= max_points:
        return unique_points
    sampled = [unique_points[0]]
    last_index = len(unique_points) - 1
    for index in range(1, max_points - 1):
        sampled_index = round(index * last_index / (max_points - 1))
        sampled.append(unique_points[sampled_index])
    sampled.append(unique_points[-1])
    deduped: list[float] = []
    for point in sampled:
        if not deduped or point != deduped[-1]:
            deduped.append(point)
    return deduped


def _prediction_order_entries(storage: Storage) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for row in storage.list_open_orders():
        created_at = _parse_iso_timestamp(str(row["created_at"]))
        if created_at is None:
            continue
        entries.append(
            {
                "market_id": str(row["market_id"]),
                "asset_id": int(row["asset_id"]),
                "opened_at": created_at.timestamp(),
                "entry_price": float(row["filled_price"] if row["filled_price"] is not None else row["price"]),
                "quantity": float(row["quantity"]),
            }
        )
    return entries


def _perp_order_entries(storage: Storage) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for row in storage.list_open_perp_orders():
        created_at = _parse_iso_timestamp(str(row["created_at"]))
        if created_at is None:
            continue
        entries.append(
            {
                "market_id": str(row["market_id"]),
                "asset_id": int(row["asset_id"]),
                "opened_at": created_at.timestamp(),
                "entry_price": float(row["filled_price"] if row["filled_price"] is not None else row["price"]),
                "quantity": float(row["quantity"]),
                "direction": 1.0 if str(row["side"]).lower() == "buy" else -1.0,
            }
        )
    return entries


def _pnl_series(storage: Storage) -> dict[str, Any]:
    prediction_orders = _prediction_order_entries(storage)
    perp_orders = _perp_order_entries(storage)
    prediction_keys = {(entry["market_id"], entry["asset_id"]) for entry in prediction_orders}
    perp_keys = {(entry["market_id"], entry["asset_id"]) for entry in perp_orders}
    prediction_events = _fetch_mark_rows(storage, "book_events", prediction_keys)
    perp_events = _fetch_mark_rows(storage, "perp_book_events", perp_keys)

    now_ts = datetime.now(tz=timezone.utc).timestamp()
    timeline = [entry["opened_at"] for entry in prediction_orders]
    timeline.extend(entry["opened_at"] for entry in perp_orders)
    timeline.extend(row["timestamp_seconds"] for row in prediction_events)
    timeline.extend(row["timestamp_seconds"] for row in perp_events)
    timeline.append(now_ts)
    sampled_timeline = _sample_timestamps(timeline, MAX_PNL_SERIES_POINTS)
    if not sampled_timeline:
        sampled_timeline = [now_ts]

    prediction_events.sort(key=lambda row: row["timestamp_seconds"])
    perp_events.sort(key=lambda row: row["timestamp_seconds"])
    prediction_index = 0
    perp_index = 0
    latest_prediction_marks: dict[tuple[str, int], float] = {}
    latest_perp_marks: dict[tuple[str, int], float] = {}
    points: list[dict[str, Any]] = []
    for point in sampled_timeline:
        while prediction_index < len(prediction_events) and prediction_events[prediction_index]["timestamp_seconds"] <= point:
            row = prediction_events[prediction_index]
            latest_prediction_marks[row["key"]] = float(row["mid_price"])
            prediction_index += 1
        while perp_index < len(perp_events) and perp_events[perp_index]["timestamp_seconds"] <= point:
            row = perp_events[perp_index]
            latest_perp_marks[row["key"]] = float(row["mid_price"])
            perp_index += 1

        prediction_pnl = 0.0
        for entry in prediction_orders:
            if entry["opened_at"] > point:
                continue
            mark = latest_prediction_marks.get((entry["market_id"], entry["asset_id"]))
            if mark is None:
                continue
            prediction_pnl += entry["quantity"] * (mark - entry["entry_price"])

        perp_pnl = 0.0
        for entry in perp_orders:
            if entry["opened_at"] > point:
                continue
            mark = latest_perp_marks.get((entry["market_id"], entry["asset_id"]))
            if mark is None:
                continue
            perp_pnl += entry["quantity"] * (mark - entry["entry_price"]) * entry["direction"]

        points.append(
            {
                "timestamp": datetime.fromtimestamp(point, tz=timezone.utc).isoformat(),
                "prediction_pnl_usdh": prediction_pnl,
                "perp_pnl_usdh": perp_pnl,
                "total_pnl_usdh": prediction_pnl + perp_pnl,
            }
        )

    return {"mode": "mark_to_market_open_positions", "points": points}


def _followed_wallet_addresses(storage: Storage) -> set[str]:
    statuses = ", ".join("?" for _ in OPEN_ORDER_STATUSES)
    addresses: set[str] = set()
    with storage.connect() as connection:
        prediction_rows = connection.execute(
            f"""
            SELECT whale_signals.details_json
            FROM orders
            INNER JOIN whale_signals ON whale_signals.id = orders.signal_id
            WHERE orders.status IN ({statuses}) AND whale_signals.wallet_bonus_applied = 1
            """,
            OPEN_ORDER_STATUSES,
        ).fetchall()
        perp_rows = connection.execute(
            f"""
            SELECT perp_whale_signals.details_json
            FROM perp_orders
            INNER JOIN perp_whale_signals ON perp_whale_signals.id = perp_orders.signal_id
            WHERE perp_orders.status IN ({statuses}) AND perp_whale_signals.wallet_bonus_applied = 1
            """,
            OPEN_ORDER_STATUSES,
        ).fetchall()
    for row in [*prediction_rows, *perp_rows]:
        details = json.loads(row["details_json"])
        address = details.get("trigger_address")
        if isinstance(address, str) and address:
            addresses.add(address.lower())
    return addresses


async def _load_wallet_recent_trades(addresses: tuple[str, ...]) -> dict[str, list[dict[str, Any]]]:
    config = Config.from_env()
    adapter = HyperliquidAdapter(config)
    results: dict[str, list[dict[str, Any]]] = {}
    try:
        for address in addresses:
            try:
                fills = await adapter.fetch_user_fills_by_time(address)
            except Exception:
                results[address.lower()] = []
                continue
            recent = sorted(fills, key=lambda fill: int(fill.get("time", 0) or 0), reverse=True)[:5]
            results[address.lower()] = [
                {
                    "coin": fill.get("coin"),
                    "side": fill.get("side"),
                    "price": float(fill.get("px", 0.0) or 0.0),
                    "size": float(fill.get("sz", 0.0) or 0.0),
                    "closed_pnl": float(fill.get("closedPnl", 0.0) or 0.0),
                    "time": _normalize_unix_timestamp(int(fill.get("time", 0) or 0)),
                }
                for fill in recent
            ]
    finally:
        await adapter.close()
    return results


def _wallet_recent_trades(addresses: tuple[str, ...]) -> dict[str, list[dict[str, Any]]]:
    now = time.time()
    cached_addresses = tuple(_WHALE_SPOTLIGHT_CACHE.get("addresses", tuple()))
    cached_data = dict(_WHALE_SPOTLIGHT_CACHE.get("data", {}))
    if now - float(_WHALE_SPOTLIGHT_CACHE.get("loaded_at", 0.0)) < WHALE_SPOTLIGHT_CACHE_TTL_SECONDS and cached_addresses == addresses:
        return cached_data
    try:
        data = asyncio.run(_load_wallet_recent_trades(addresses)) if addresses else {}
    except Exception:
        return cached_data
    _WHALE_SPOTLIGHT_CACHE["loaded_at"] = now
    _WHALE_SPOTLIGHT_CACHE["addresses"] = addresses
    _WHALE_SPOTLIGHT_CACHE["data"] = data
    return data


def _signal_linked_position_map(prediction_positions: list[dict], perp_positions: list[dict]) -> dict[int, dict[str, Any]]:
    signal_map: dict[int, dict[str, Any]] = {}
    for position in [*prediction_positions, *perp_positions]:
        signal_id = position.get("signal_id")
        if isinstance(signal_id, int):
            signal_map[signal_id] = position
    return signal_map


def _following_positions_by_wallet(storage: Storage, prediction_positions: list[dict], perp_positions: list[dict]) -> dict[str, list[dict[str, Any]]]:
    positions_by_wallet: dict[str, list[dict[str, Any]]] = defaultdict(list)
    signal_map = _signal_linked_position_map(prediction_positions, perp_positions)
    statuses = ", ".join("?" for _ in OPEN_ORDER_STATUSES)
    with storage.connect() as connection:
        prediction_rows = connection.execute(
            f"""
            SELECT whale_signals.id, whale_signals.details_json
            FROM orders
            INNER JOIN whale_signals ON whale_signals.id = orders.signal_id
            WHERE orders.status IN ({statuses}) AND whale_signals.wallet_bonus_applied = 1
            """,
            OPEN_ORDER_STATUSES,
        ).fetchall()
        perp_rows = connection.execute(
            f"""
            SELECT perp_whale_signals.id, perp_whale_signals.details_json
            FROM perp_orders
            INNER JOIN perp_whale_signals ON perp_whale_signals.id = perp_orders.signal_id
            WHERE perp_orders.status IN ({statuses}) AND perp_whale_signals.wallet_bonus_applied = 1
            """,
            OPEN_ORDER_STATUSES,
        ).fetchall()
    for row in [*prediction_rows, *perp_rows]:
        details = json.loads(row["details_json"])
        address = details.get("trigger_address")
        signal_id = int(row["id"])
        if not isinstance(address, str) or not address:
            continue
        position = signal_map.get(signal_id)
        if position is None:
            continue
        positions_by_wallet[address.lower()].append(position)
    return dict(positions_by_wallet)


def _whale_spotlight(storage: Storage, prediction_positions: list[dict], perp_positions: list[dict]) -> list[dict[str, Any]]:
    rows = storage.list_wallet_scores()
    followed_wallets = _followed_wallet_addresses(storage)
    following_positions = _following_positions_by_wallet(storage, prediction_positions, perp_positions)
    spotlight_rows = [
        row
        for row in rows
        if int(row["trade_count"]) >= 8 and float(row["total_pnl_usdh"]) >= 100.0
    ][:3]
    addresses = tuple(str(row["address"]) for row in spotlight_rows)
    recent_trades = _wallet_recent_trades(addresses)
    now_ts = int(datetime.now(tz=timezone.utc).timestamp())
    cards: list[dict[str, Any]] = []
    for row in spotlight_rows:
        address = str(row["address"])
        normalized_address = address.lower()
        last_trade_ts = _normalize_unix_timestamp(int(row["last_trade_ts"])) if row["last_trade_ts"] is not None else None
        cards.append(
            {
                "address": address,
                "trade_count": int(row["trade_count"]),
                "win_rate": float(row["win_rate"]),
                "total_pnl_usdh": float(row["total_pnl_usdh"]),
                "active": isinstance(last_trade_ts, int) and last_trade_ts >= now_ts - 86_400,
                "following": normalized_address in followed_wallets,
                "recent_trades": recent_trades.get(normalized_address, []),
                "current_open_positions": following_positions.get(normalized_address, []),
            }
        )
    return cards


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
    market_meta = _prediction_market_meta()
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
        meta = market_meta.get(str(row["market_id"]), {})
        expiry_dt = _parse_expiry_timestamp(meta.get("expiry"))
        remaining_seconds = None
        time_remaining_fraction = None
        if expiry_dt is not None:
            remaining_seconds = int((expiry_dt - datetime.now(tz=timezone.utc)).total_seconds())
            if created_at is not None:
                total_lifetime = (expiry_dt - created_at).total_seconds()
                if total_lifetime > 0:
                    time_remaining_fraction = min(max(remaining_seconds / total_lifetime, 0.0), 1.0)
        positions.append(
            {
                "market_type": "prediction",
                "market_id": row["market_id"],
                "market_name": meta.get("title") or str(row["market_id"]),
                "asset_id": row["asset_id"],
                "signal_id": row["signal_id"],
                "contract_side": row["signal_side"] or row["side"],
                "status": row["status"],
                "entry_price": entry_price,
                "current_mark_price": current_mark,
                "quantity": quantity,
                "size_usdh": float(row["size_usdh"]),
                "unrealized_pnl_usdh": unrealized_pnl,
                "created_at": row["created_at"],
                "age_seconds": age_seconds,
                "expiry": meta.get("expiry"),
                "time_remaining_seconds": remaining_seconds,
                "time_remaining_fraction": time_remaining_fraction,
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
                "market_name": row["signal_coin"] or row["market_id"],
                "coin": row["signal_coin"] or row["market_id"],
                "signal_id": row["signal_id"],
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


def _resolved_market_map(storage: Storage) -> dict[str, dict[str, Any]]:
    resolved: dict[str, dict[str, Any]] = {}
    for row in storage.list_resolved_markets():
        details = _json_object(row["details_json"])
        resolved_at = _parse_iso_timestamp(str(row["resolved_at"]))
        winning_side = str(row["winning_side"] or details.get("winning_side") or "").upper() or None
        settlement_price = row["settlement_price"]
        if settlement_price is None:
            settlement_price = details.get("settlement_price")
        resolved[str(row["market_id"])] = {
            "resolved_at": resolved_at,
            "winning_side": winning_side,
            "settlement_price": float(settlement_price) if isinstance(settlement_price, (int, float)) else None,
        }
    return resolved


def _build_mark_series_from_rows(rows: list[Any]) -> dict[tuple[str, int], dict[str, list[float]]]:
    series: dict[tuple[str, int], dict[str, list[float]]] = defaultdict(lambda: {"times": [], "mid_prices": []})
    for row in rows:
        timestamp = _parse_iso_timestamp(str(row["timestamp"]))
        if timestamp is None or row["mid_price"] is None:
            continue
        key = (str(row["market_id"]), int(row["asset_id"]))
        series[key]["times"].append(timestamp.timestamp())
        series[key]["mid_prices"].append(float(row["mid_price"]))
    return dict(series)


def _latest_mid_at_or_before(series: dict[str, list[float]] | None, timestamp_seconds: float) -> float | None:
    if not series:
        return None
    times = series["times"]
    mid_prices = series["mid_prices"]
    index = bisect_right(times, timestamp_seconds) - 1
    if index < 0:
        return None
    return float(mid_prices[index])


def _prediction_ledger_positions(storage: Storage, latest_marks: dict[tuple[str, int], dict]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    market_meta = _prediction_market_meta()
    resolved_markets = _resolved_market_map(storage)
    open_positions: list[dict[str, Any]] = []
    closed_positions: list[dict[str, Any]] = []
    histories: list[dict[str, Any]] = []
    for row in storage.list_order_history():
        market_id = str(row["market_id"])
        asset_id = int(row["asset_id"])
        status = str(row["status"])
        created_at = _parse_iso_timestamp(str(row["created_at"]))
        if created_at is None:
            continue
        updated_at = _parse_iso_timestamp(str(row["updated_at"])) or created_at
        signal_details = _json_object(row["signal_details_json"])
        order_details = _json_object(row["order_details_json"])
        contract_side = str(row["signal_side"] or signal_details.get("contract_side") or signal_details.get("side") or "").upper() or "UNKNOWN"
        entry_price = float(row["filled_price"] if row["filled_price"] is not None else row["price"])
        quantity = float(row["quantity"])
        trigger_type = str(row["signal_trigger_type"] or signal_details.get("trigger_type") or "")
        wallet_bonus_applied = bool(row["signal_wallet_bonus_applied"]) if row["signal_wallet_bonus_applied"] is not None else bool(signal_details.get("wallet_bonus_applied"))
        coin = signal_details.get("coin") if isinstance(signal_details.get("coin"), str) else None
        meta = market_meta.get(market_id, {})
        resolved_market = resolved_markets.get(market_id)
        closed_at: datetime | None = None
        exit_price: float | None = None
        if resolved_market is not None and resolved_market.get("resolved_at") is not None:
            closed_at = resolved_market["resolved_at"]
            winning_side = resolved_market.get("winning_side")
            settlement_price = resolved_market.get("settlement_price")
            if winning_side in {"YES", "NO"} and contract_side in {"YES", "NO"}:
                exit_price = 1.0 if winning_side == contract_side else 0.0
            elif isinstance(settlement_price, (int, float)):
                exit_price = float(settlement_price)
        elif status in CLOSED_ORDER_STATUSES:
            closed_at = updated_at
            settlement_price = order_details.get("settlement_price")
            winning_side = str(order_details.get("winning_side") or signal_details.get("winning_side") or "").upper()
            if winning_side in {"YES", "NO"} and contract_side in {"YES", "NO"}:
                exit_price = 1.0 if winning_side == contract_side else 0.0
            elif isinstance(settlement_price, (int, float)):
                exit_price = float(settlement_price)
            else:
                exit_price = float(row["filled_price"] if row["filled_price"] is not None else row["price"])

        if closed_at is not None:
            final_exit_price = entry_price if exit_price is None else exit_price
            realized_pnl = quantity * (final_exit_price - entry_price)
            closed_positions.append(
                {
                    "market_id": market_id,
                    "coin": coin,
                    "market_type": "prediction",
                    "direction": contract_side,
                    "entry_price": entry_price,
                    "exit_price": final_exit_price,
                    "size_usdh": float(row["size_usdh"]),
                    "realized_pnl_usdh": realized_pnl,
                    "opened_at": _to_iso(created_at),
                    "closed_at": _to_iso(closed_at),
                    "duration_hours": _hours_between(created_at, closed_at),
                    "trigger_type": trigger_type,
                    "wallet_bonus_applied": wallet_bonus_applied,
                }
            )
            histories.append(
                {
                    "market_type": "prediction",
                    "key": (market_id, asset_id),
                    "opened_at": created_at,
                    "closed_at": closed_at,
                    "entry_price": entry_price,
                    "quantity": quantity,
                    "direction_sign": 1.0,
                    "realized_pnl_usdh": realized_pnl,
                }
            )
            continue

        if status not in OPEN_ORDER_STATUSES:
            continue

        latest_mark = latest_marks.get((market_id, asset_id))
        mark_price = latest_mark.get("mid_price") if latest_mark is not None else None
        unrealized_pnl = quantity * (float(mark_price) - entry_price) if isinstance(mark_price, (int, float)) else None
        open_positions.append(
            {
                "market_id": market_id,
                "coin": coin,
                "market_type": "prediction",
                "direction": contract_side,
                "entry_price": entry_price,
                "mark_price": mark_price,
                "size_usdh": float(row["size_usdh"]),
                "unrealized_pnl_usdh": unrealized_pnl,
                "opened_at": _to_iso(created_at),
                "age_hours": _hours_between(created_at, datetime.now(tz=timezone.utc)),
                "trigger_type": trigger_type,
                "wallet_bonus_applied": wallet_bonus_applied,
                "expiry": meta.get("expiry"),
            }
        )
        histories.append(
            {
                "market_type": "prediction",
                "key": (market_id, asset_id),
                "opened_at": created_at,
                "closed_at": None,
                "entry_price": entry_price,
                "quantity": quantity,
                "direction_sign": 1.0,
                "realized_pnl_usdh": 0.0,
            }
        )

    open_positions.sort(key=lambda item: item["opened_at"] or "", reverse=True)
    closed_positions.sort(key=lambda item: item["closed_at"] or "", reverse=True)
    return open_positions, closed_positions, histories


def _perp_direction_from_order_side(order_side: str) -> tuple[float, str]:
    normalized = order_side.lower()
    if normalized == "sell":
        return -1.0, "short"
    return 1.0, "long"


def _perp_ledger_positions(storage: Storage, latest_marks: dict[tuple[str, int], dict]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    open_positions: list[dict[str, Any]] = []
    closed_positions: list[dict[str, Any]] = []
    histories: list[dict[str, Any]] = []
    active_positions: dict[str, dict[str, Any]] = {}

    for row in storage.list_perp_order_history():
        market_id = str(row["market_id"])
        asset_id = int(row["asset_id"])
        status = str(row["status"])
        created_at = _parse_iso_timestamp(str(row["created_at"]))
        if created_at is None:
            continue
        updated_at = _parse_iso_timestamp(str(row["updated_at"])) or created_at
        order_details = _json_object(row["order_details_json"])
        signal_details = _json_object(row["signal_details_json"])
        entry_price = float(row["filled_price"] if row["filled_price"] is not None else row["price"])
        quantity = float(row["quantity"])
        direction_sign, fallback_direction = _perp_direction_from_order_side(str(row["side"]))
        direction = str(row["signal_side"] or signal_details.get("side") or fallback_direction)
        active_position = active_positions.get(market_id)

        if active_position is None:
            position = {
                "market_id": market_id,
                "asset_id": asset_id,
                "coin": str(row["signal_coin"] or order_details.get("coin") or signal_details.get("coin") or market_id),
                "direction": direction,
                "order_side": str(row["side"]).lower(),
                "direction_sign": direction_sign,
                "entry_price": entry_price,
                "quantity": quantity,
                "size_usdh": float(row["size_usdh"]),
                "opened_at": created_at,
                "trigger_type": str(row["signal_trigger_type"] or signal_details.get("trigger_type") or ""),
                "wallet_bonus_applied": bool(row["signal_wallet_bonus_applied"]) if row["signal_wallet_bonus_applied"] is not None else bool(signal_details.get("wallet_bonus_applied")),
            }
            if status in CLOSED_ORDER_STATUSES:
                exit_price_raw = order_details.get("settlement_price")
                exit_price = float(exit_price_raw) if isinstance(exit_price_raw, (int, float)) else entry_price
                realized_pnl = quantity * (exit_price - entry_price) * direction_sign
                closed_positions.append(
                    {
                        "market_id": market_id,
                        "coin": position["coin"],
                        "market_type": "perp",
                        "direction": direction,
                        "entry_price": entry_price,
                        "exit_price": exit_price,
                        "size_usdh": float(row["size_usdh"]),
                        "realized_pnl_usdh": realized_pnl,
                        "opened_at": _to_iso(created_at),
                        "closed_at": _to_iso(updated_at),
                        "duration_hours": _hours_between(created_at, updated_at),
                        "trigger_type": position["trigger_type"],
                        "wallet_bonus_applied": position["wallet_bonus_applied"],
                    }
                )
                histories.append(
                    {
                        "market_type": "perp",
                        "key": (market_id, asset_id),
                        "opened_at": created_at,
                        "closed_at": updated_at,
                        "entry_price": entry_price,
                        "quantity": quantity,
                        "direction_sign": direction_sign,
                        "realized_pnl_usdh": realized_pnl,
                    }
                )
            elif status in OPEN_ORDER_STATUSES:
                active_positions[market_id] = position
            continue

        if str(row["side"]).lower() == active_position["order_side"]:
            total_quantity = active_position["quantity"] + quantity
            if total_quantity > 0:
                active_position["entry_price"] = (
                    (active_position["entry_price"] * active_position["quantity"]) + (entry_price * quantity)
                ) / total_quantity
                active_position["quantity"] = total_quantity
                active_position["size_usdh"] += float(row["size_usdh"])
            if status in CLOSED_ORDER_STATUSES:
                exit_price_raw = order_details.get("settlement_price")
                exit_price = float(exit_price_raw) if isinstance(exit_price_raw, (int, float)) else active_position["entry_price"]
                realized_pnl = active_position["quantity"] * (exit_price - active_position["entry_price"]) * active_position["direction_sign"]
                closed_positions.append(
                    {
                        "market_id": market_id,
                        "coin": active_position["coin"],
                        "market_type": "perp",
                        "direction": active_position["direction"],
                        "entry_price": active_position["entry_price"],
                        "exit_price": exit_price,
                        "size_usdh": active_position["size_usdh"],
                        "realized_pnl_usdh": realized_pnl,
                        "opened_at": _to_iso(active_position["opened_at"]),
                        "closed_at": _to_iso(updated_at),
                        "duration_hours": _hours_between(active_position["opened_at"], updated_at),
                        "trigger_type": active_position["trigger_type"],
                        "wallet_bonus_applied": active_position["wallet_bonus_applied"],
                    }
                )
                histories.append(
                    {
                        "market_type": "perp",
                        "key": (market_id, asset_id),
                        "opened_at": active_position["opened_at"],
                        "closed_at": updated_at,
                        "entry_price": active_position["entry_price"],
                        "quantity": active_position["quantity"],
                        "direction_sign": active_position["direction_sign"],
                        "realized_pnl_usdh": realized_pnl,
                    }
                )
                del active_positions[market_id]
            continue

        exit_price_raw = order_details.get("settlement_price")
        exit_price = float(exit_price_raw) if isinstance(exit_price_raw, (int, float)) else entry_price
        realized_pnl = active_position["quantity"] * (exit_price - active_position["entry_price"]) * active_position["direction_sign"]
        closed_positions.append(
            {
                "market_id": market_id,
                "coin": active_position["coin"],
                "market_type": "perp",
                "direction": active_position["direction"],
                "entry_price": active_position["entry_price"],
                "exit_price": exit_price,
                "size_usdh": active_position["size_usdh"],
                "realized_pnl_usdh": realized_pnl,
                "opened_at": _to_iso(active_position["opened_at"]),
                "closed_at": _to_iso(created_at),
                "duration_hours": _hours_between(active_position["opened_at"], created_at),
                "trigger_type": active_position["trigger_type"],
                "wallet_bonus_applied": active_position["wallet_bonus_applied"],
            }
        )
        histories.append(
            {
                "market_type": "perp",
                "key": (market_id, active_position["asset_id"]),
                "opened_at": active_position["opened_at"],
                "closed_at": created_at,
                "entry_price": active_position["entry_price"],
                "quantity": active_position["quantity"],
                "direction_sign": active_position["direction_sign"],
                "realized_pnl_usdh": realized_pnl,
            }
        )
        del active_positions[market_id]

    now = datetime.now(tz=timezone.utc)
    for active_position in active_positions.values():
        latest_mark = latest_marks.get((active_position["market_id"], active_position["asset_id"]))
        mark_price = latest_mark.get("mid_price") if latest_mark is not None else None
        unrealized_pnl = None
        if isinstance(mark_price, (int, float)):
            unrealized_pnl = active_position["quantity"] * (float(mark_price) - active_position["entry_price"]) * active_position["direction_sign"]
        open_positions.append(
            {
                "market_id": active_position["market_id"],
                "coin": active_position["coin"],
                "market_type": "perp",
                "direction": active_position["direction"],
                "entry_price": active_position["entry_price"],
                "mark_price": mark_price,
                "size_usdh": active_position["size_usdh"],
                "unrealized_pnl_usdh": unrealized_pnl,
                "opened_at": _to_iso(active_position["opened_at"]),
                "age_hours": _hours_between(active_position["opened_at"], now),
                "trigger_type": active_position["trigger_type"],
                "wallet_bonus_applied": active_position["wallet_bonus_applied"],
                "expiry": None,
            }
        )
        histories.append(
            {
                "market_type": "perp",
                "key": (active_position["market_id"], active_position["asset_id"]),
                "opened_at": active_position["opened_at"],
                "closed_at": None,
                "entry_price": active_position["entry_price"],
                "quantity": active_position["quantity"],
                "direction_sign": active_position["direction_sign"],
                "realized_pnl_usdh": 0.0,
            }
        )

    open_positions.sort(key=lambda item: item["opened_at"] or "", reverse=True)
    closed_positions.sort(key=lambda item: item["closed_at"] or "", reverse=True)
    return open_positions, closed_positions, histories


def _hour_floor(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def _ledger_equity_curve(storage: Storage, starting_cash_usdh: float, histories: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not histories:
        now = _hour_floor(datetime.now(tz=timezone.utc))
        return [{"timestamp": _to_iso(now), "equity": starting_cash_usdh}]

    prediction_keys = {history["key"] for history in histories if history["market_type"] == "prediction"}
    perp_keys = {history["key"] for history in histories if history["market_type"] == "perp"}
    prediction_series = _build_mark_series_from_rows(storage.list_prediction_mark_history(prediction_keys))
    perp_series = _build_mark_series_from_rows(storage.list_perp_mark_history(perp_keys))

    mark_timestamps: list[datetime] = []
    for series in [*prediction_series.values(), *perp_series.values()]:
        if not series["times"]:
            continue
        mark_timestamps.append(datetime.fromtimestamp(series["times"][-1], tz=timezone.utc))

    start_at = _hour_floor(min(history["opened_at"] for history in histories))
    end_candidates = [datetime.now(tz=timezone.utc)]
    end_candidates.extend(mark_timestamps)
    end_candidates.extend(history["closed_at"] for history in histories if history["closed_at"] is not None)
    end_at = _hour_floor(max(end_candidates))

    points: list[dict[str, Any]] = []
    cursor = start_at
    while cursor <= end_at:
        point_ts = cursor.timestamp()
        realized_pnl = sum(
            float(history["realized_pnl_usdh"])
            for history in histories
            if history["closed_at"] is not None and history["closed_at"].timestamp() <= point_ts
        )
        unrealized_pnl = 0.0
        for history in histories:
            if history["opened_at"].timestamp() > point_ts:
                continue
            if history["closed_at"] is not None and history["closed_at"].timestamp() <= point_ts:
                continue
            mark_series = prediction_series.get(history["key"]) if history["market_type"] == "prediction" else perp_series.get(history["key"])
            mark_price = _latest_mid_at_or_before(mark_series, point_ts)
            if mark_price is None:
                continue
            if history["market_type"] == "prediction":
                unrealized_pnl += float(history["quantity"]) * (mark_price - float(history["entry_price"]))
            else:
                unrealized_pnl += float(history["quantity"]) * (mark_price - float(history["entry_price"])) * float(history["direction_sign"])
        points.append(
            {
                "timestamp": _to_iso(cursor),
                "equity": starting_cash_usdh + realized_pnl + unrealized_pnl,
            }
        )
        cursor += timedelta(hours=1)

    return points


def _daily_pnl_from_equity_curve(equity_curve: list[dict[str, Any]]) -> float:
    if not equity_curve:
        return 0.0
    latest_point = equity_curve[-1]
    latest_timestamp = _parse_iso_timestamp(str(latest_point["timestamp"]))
    if latest_timestamp is None:
        return 0.0
    cutoff = latest_timestamp - timedelta(hours=24)
    baseline_point = equity_curve[0]
    for point in equity_curve:
        point_ts = _parse_iso_timestamp(str(point["timestamp"]))
        if point_ts is None:
            continue
        if point_ts <= cutoff:
            baseline_point = point
        else:
            break
    return float(latest_point["equity"]) - float(baseline_point["equity"])


def _ledger_payload(storage: Storage) -> dict[str, Any]:
    prediction_marks = _latest_prediction_marks(storage)
    perp_marks = _latest_perp_marks(storage)
    prediction_open, prediction_closed, prediction_histories = _prediction_ledger_positions(storage, prediction_marks)
    perp_open, perp_closed, perp_histories = _perp_ledger_positions(storage, perp_marks)
    open_positions = sorted([*prediction_open, *perp_open], key=lambda item: item["opened_at"] or "", reverse=True)
    closed_positions = sorted([*prediction_closed, *perp_closed], key=lambda item: item["closed_at"] or "", reverse=True)
    starting_cash_usdh = _ledger_starting_cash(storage)
    realized_pnl_usdh = sum(float(position["realized_pnl_usdh"]) for position in closed_positions)
    unrealized_pnl_usdh = sum(float(position["unrealized_pnl_usdh"] or 0.0) for position in open_positions)
    total_equity_usdh = starting_cash_usdh + realized_pnl_usdh + unrealized_pnl_usdh
    winning_positions = [position for position in closed_positions if float(position["realized_pnl_usdh"]) > 0]
    losing_positions = [position for position in closed_positions if float(position["realized_pnl_usdh"]) < 0]
    gross_profit = sum(float(position["realized_pnl_usdh"]) for position in winning_positions)
    gross_loss = sum(float(position["realized_pnl_usdh"]) for position in losing_positions)
    equity_curve = _ledger_equity_curve(storage, starting_cash_usdh, [*prediction_histories, *perp_histories])
    closed_count = len(closed_positions)
    total_return_pct = (total_equity_usdh - starting_cash_usdh) / starting_cash_usdh * 100 if starting_cash_usdh > 0 else 0.0

    return {
        "summary": {
            "starting_cash_usdh": starting_cash_usdh,
            "realized_pnl_usdh": realized_pnl_usdh,
            "unrealized_pnl_usdh": unrealized_pnl_usdh,
            "total_equity_usdh": total_equity_usdh,
            "total_return_pct": total_return_pct,
            "closed_positions": closed_count,
            "winning_positions": len(winning_positions),
            "losing_positions": len(losing_positions),
            "win_rate_pct": (len(winning_positions) / closed_count * 100) if closed_count else 0.0,
            "avg_win_usdh": (gross_profit / len(winning_positions)) if winning_positions else 0.0,
            "avg_loss_usdh": (gross_loss / len(losing_positions)) if losing_positions else 0.0,
            "profit_factor": (gross_profit / abs(gross_loss)) if gross_loss < 0 else 0.0,
            "best_trade_usdh": max((float(position["realized_pnl_usdh"]) for position in closed_positions), default=0.0),
            "worst_trade_usdh": min((float(position["realized_pnl_usdh"]) for position in closed_positions), default=0.0),
            "daily_pnl_usdh": _daily_pnl_from_equity_curve(equity_curve),
        },
        "equity_curve": equity_curve,
        "closed_positions": closed_positions,
        "open_positions": open_positions,
    }


def _wallet_address_key(address: str) -> str:
    return address.strip().lower()


def _parse_cluster_window_seconds(value: str | None) -> int:
    if value is None:
        return CLUSTER_WINDOW_SECONDS
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return CLUSTER_WINDOW_SECONDS
    return max(CLUSTER_WINDOW_MIN_SECONDS, min(CLUSTER_WINDOW_MAX_SECONDS, parsed))


def _parse_cluster_levels(levels_json: str | None) -> tuple[tuple[float, float], ...]:
    if not levels_json:
        return ()
    try:
        payload = json.loads(levels_json)
    except (TypeError, ValueError):
        return ()
    levels: list[tuple[float, float]] = []
    if not isinstance(payload, list):
        return ()
    for level in payload:
        if not isinstance(level, dict):
            continue
        price = level.get("price", level.get("px"))
        size = level.get("size", level.get("sz"))
        if price in (None, "") or size in (None, ""):
            continue
        try:
            levels.append((float(price), float(size)))
        except (TypeError, ValueError):
            continue
    return tuple(levels)


def _cluster_size_at_price(levels: tuple[tuple[float, float], ...], price: float) -> float:
    for level_price, level_size in levels:
        if level_price == price:
            return level_size
    return 0.0


def _cluster_book_imbalance(bids: tuple[tuple[float, float], ...], asks: tuple[tuple[float, float], ...]) -> float:
    bid_notional = sum(price * size for price, size in bids[:3])
    ask_notional = sum(price * size for price, size in asks[:3])
    total = bid_notional + ask_notional
    if total <= 0:
        return 0.5
    return bid_notional / total


def _derive_perp_cluster_side(
    previous_row: Any | None,
    current_row: Any,
) -> str:
    current_bids = _parse_cluster_levels(current_row["bids_json"])
    current_asks = _parse_cluster_levels(current_row["asks_json"])
    if previous_row is not None:
        previous_bids = _parse_cluster_levels(previous_row["bids_json"])
        previous_asks = _parse_cluster_levels(previous_row["asks_json"])
        long_added = 0.0
        short_added = 0.0
        if current_bids:
            current_bid_price, current_bid_size = current_bids[0]
            previous_bid_size = _cluster_size_at_price(previous_bids, current_bid_price)
            long_added = current_bid_price * max(current_bid_size - previous_bid_size, 0.0)
        if current_asks:
            current_ask_price, current_ask_size = current_asks[0]
            previous_ask_size = _cluster_size_at_price(previous_asks, current_ask_price)
            short_added = current_ask_price * max(current_ask_size - previous_ask_size, 0.0)
        if long_added > short_added:
            return "long"
        if short_added > long_added:
            return "short"

        previous_mid = float(previous_row["mid_price"]) if previous_row["mid_price"] is not None else None
        current_mid = float(current_row["mid_price"]) if current_row["mid_price"] is not None else None
        if previous_mid is not None and current_mid is not None and previous_mid > 0:
            price_impact = (current_mid - previous_mid) / previous_mid
            if price_impact > 0:
                return "long"
            if price_impact < 0:
                return "short"

    return "long" if _cluster_book_imbalance(current_bids, current_asks) >= 0.5 else "short"


def _collapse_cluster_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    collapsed: list[dict[str, Any]] = []
    last_seen_by_key: dict[tuple[str, str, int, str, str], datetime] = {}
    for event in sorted(
        events,
        key=lambda item: (item["market_type"], item["market_id"], item["asset_id"], item["side"], item["timestamp"]),
    ):
        key = (
            event["market_type"],
            event["market_id"],
            int(event["asset_id"]),
            event["side"],
            event["address_key"],
        )
        previous_timestamp = last_seen_by_key.get(key)
        if previous_timestamp is not None and (event["timestamp"] - previous_timestamp).total_seconds() <= CLUSTER_COLLAPSE_SECONDS:
            continue
        last_seen_by_key[key] = event["timestamp"]
        collapsed.append(event)
    return collapsed


def _cluster_prediction_events(storage: Storage) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for row in storage.list_cluster_book_events():
        address = str(row["trigger_address"] or "").strip()
        if not address:
            continue
        timestamp = _parse_iso_timestamp(str(row["timestamp"]))
        if timestamp is None:
            continue
        events.append(
            {
                "market_type": "prediction",
                "market_id": str(row["market_id"]),
                "asset_id": int(row["asset_id"]),
                "side": str(row["outcome_side"]),
                "timestamp": timestamp,
                "address": address,
                "address_key": _wallet_address_key(address),
                "coin": None,
            }
        )
    return _collapse_cluster_events(events)


def _cluster_perp_events(storage: Storage) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    previous_rows: dict[tuple[str, int], Any] = {}
    for row in storage.list_cluster_perp_book_events():
        key = (str(row["market_id"]), int(row["asset_id"]))
        previous_row = previous_rows.get(key)
        previous_rows[key] = row
        address = str(row["trigger_address"] or "").strip()
        if not address:
            continue
        timestamp = _parse_iso_timestamp(str(row["timestamp"]))
        if timestamp is None:
            continue
        events.append(
            {
                "market_type": "perp",
                "market_id": str(row["market_id"]),
                "asset_id": int(row["asset_id"]),
                "side": _derive_perp_cluster_side(previous_row, row),
                "timestamp": timestamp,
                "address": address,
                "address_key": _wallet_address_key(address),
                "coin": str(row["coin"]),
            }
        )
    return _collapse_cluster_events(events)


def _cluster_wallet_score_map(storage: Storage) -> dict[str, dict[str, Any]]:
    scores: dict[str, dict[str, Any]] = {}
    for row in storage.list_wallet_scores():
        address = str(row["address"])
        scores[_wallet_address_key(address)] = {
            "address": address,
            "win_rate": float(row["win_rate"]),
            "total_pnl_usdh": float(row["total_pnl_usdh"]),
            "trade_count": int(row["trade_count"]),
            "last_active_ts": _normalize_unix_timestamp(int(row["last_trade_ts"])) if row["last_trade_ts"] is not None else None,
        }
    return scores


def _cluster_recent_trades(events: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    recent_trades: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in sorted(events, key=lambda item: item["timestamp"], reverse=True):
        wallet_trades = recent_trades[event["address_key"]]
        if len(wallet_trades) >= CLUSTER_RECENT_TRADES_LIMIT:
            continue
        wallet_trades.append(
            {
                "market_id": event["market_id"],
                "market_type": event["market_type"],
                "coin": event["coin"],
                "side": event["side"],
                "timestamp": event["timestamp"].isoformat(),
            }
        )
    return dict(recent_trades)


def _cluster_edges(
    events: list[dict[str, Any]],
    address_display: dict[str, str],
    window_seconds: int,
) -> list[dict[str, Any]]:
    events_by_market_and_side: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        events_by_market_and_side[(event["market_id"], event["side"])].append(event)

    edge_stats: dict[tuple[str, str], dict[str, Any]] = {}
    for (market_id, _side), group in events_by_market_and_side.items():
        ordered_group = sorted(group, key=lambda item: item["timestamp"])
        for index, left in enumerate(ordered_group):
            for right in ordered_group[index + 1 :]:
                delta_seconds = (right["timestamp"] - left["timestamp"]).total_seconds()
                if delta_seconds > window_seconds:
                    break
                if left["address_key"] == right["address_key"]:
                    continue
                pair = tuple(sorted((left["address_key"], right["address_key"])))
                stat = edge_stats.setdefault(
                    pair,
                    {
                        "source": address_display.get(pair[0], pair[0]),
                        "target": address_display.get(pair[1], pair[1]),
                        "weight": 0,
                        "markets": set(),
                        "delta_sum": 0.0,
                    },
                )
                stat["weight"] += 1
                stat["markets"].add(market_id)
                stat["delta_sum"] += delta_seconds

    return [
        {
            "source": stat["source"],
            "target": stat["target"],
            "weight": int(stat["weight"]),
            "markets": sorted(stat["markets"]),
            "avg_time_delta_seconds": stat["delta_sum"] / stat["weight"],
        }
        for stat in sorted(edge_stats.values(), key=lambda item: (-item["weight"], item["source"], item["target"]))
        if int(stat["weight"]) >= CLUSTER_MIN_EDGE_WEIGHT
    ]


def _cluster_convoys(
    events: list[dict[str, Any]],
    scores_by_wallet: dict[str, dict[str, Any]],
    address_display: dict[str, str],
    window_seconds: int,
) -> list[dict[str, Any]]:
    eligible_wallets = {
        address_key: score
        for address_key, score in scores_by_wallet.items()
        if score["win_rate"] is not None
        and float(score["win_rate"]) >= CLUSTER_CONVOY_MIN_WIN_RATE
        and int(score["trade_count"]) >= CLUSTER_CONVOY_MIN_TRADE_COUNT
    }
    events_by_market_and_side: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        if event["address_key"] not in eligible_wallets:
            continue
        events_by_market_and_side[(event["market_id"], event["side"])].append(event)

    emitted_at: dict[tuple[str, str, tuple[str, ...]], datetime] = {}
    convoys: list[dict[str, Any]] = []
    for (market_id, side), group in events_by_market_and_side.items():
        ordered_group = sorted(group, key=lambda item: item["timestamp"])
        window: deque[dict[str, Any]] = deque()
        for event in ordered_group:
            window.append(event)
            cutoff_timestamp = event["timestamp"].timestamp() - window_seconds
            while window and window[0]["timestamp"].timestamp() < cutoff_timestamp:
                window.popleft()
            wallet_keys = tuple(sorted({item["address_key"] for item in window}))
            if len(wallet_keys) < 2:
                continue
            convoy_key = (market_id, side, wallet_keys)
            last_emitted_timestamp = emitted_at.get(convoy_key)
            if last_emitted_timestamp is not None and (event["timestamp"] - last_emitted_timestamp).total_seconds() <= window_seconds:
                continue
            emitted_at[convoy_key] = event["timestamp"]
            combined_win_rate = sum(float(eligible_wallets[wallet_key]["win_rate"]) for wallet_key in wallet_keys) / len(wallet_keys)
            convoys.append(
                {
                    "wallets": [address_display.get(wallet_key, wallet_key) for wallet_key in wallet_keys],
                    "market_id": market_id,
                    "side": side,
                    "timestamp": event["timestamp"].isoformat(),
                    "combined_win_rate": combined_win_rate,
                }
            )
    return sorted(convoys, key=lambda item: item["timestamp"], reverse=True)


def _wallet_clusters(storage: Storage, window_seconds: int = CLUSTER_WINDOW_SECONDS) -> dict[str, Any]:
    scores_by_wallet = _cluster_wallet_score_map(storage)
    prediction_events = _cluster_prediction_events(storage)
    perp_events = _cluster_perp_events(storage)
    all_events = sorted([*prediction_events, *perp_events], key=lambda item: item["timestamp"])

    address_display: dict[str, str] = {}
    last_active_by_wallet: dict[str, int] = {}
    for event in all_events:
        address_display.setdefault(event["address_key"], event["address"])
        last_active_by_wallet[event["address_key"]] = int(event["timestamp"].timestamp())

    edges = _cluster_edges(all_events, address_display, window_seconds)
    convoys = _cluster_convoys(all_events, scores_by_wallet, address_display, window_seconds)
    recent_trades = _cluster_recent_trades(all_events)

    relevant_wallets: set[str] = set()
    for edge in edges:
        relevant_wallets.add(_wallet_address_key(str(edge["source"])))
        relevant_wallets.add(_wallet_address_key(str(edge["target"])))
    for convoy in convoys:
        relevant_wallets.update(_wallet_address_key(str(address)) for address in convoy["wallets"])

    wallets = []
    for wallet_key in sorted(relevant_wallets):
        score = scores_by_wallet.get(wallet_key)
        wallets.append(
            {
                "address": address_display.get(wallet_key, score["address"] if score is not None else wallet_key),
                "win_rate": score["win_rate"] if score is not None else None,
                "total_pnl_usdh": score["total_pnl_usdh"] if score is not None else 0.0,
                "trade_count": score["trade_count"] if score is not None else 0,
                "last_active_ts": (
                    score["last_active_ts"]
                    if score is not None and score["last_active_ts"] is not None
                    else last_active_by_wallet.get(wallet_key)
                ),
                "recent_trades": recent_trades.get(wallet_key, []),
            }
        )

    wallets.sort(
        key=lambda item: (
            -(float(item["win_rate"]) if item["win_rate"] is not None else -1.0),
            -float(item["total_pnl_usdh"]),
            item["address"],
        )
    )
    return {
        "wallets": wallets,
        "edges": edges,
        "convoys": convoys,
    }


@app.get("/api/pnl-series")
def pnl_series() -> object:
    return jsonify(_pnl_series(get_storage()))


@app.get("/ledger")
def ledger() -> str:
    return render_template("ledger.html")


@app.get("/api/ledger")
def ledger_api() -> object:
    return jsonify(_ledger_payload(get_storage()))


@app.get("/clusters")
def clusters() -> str:
    return render_template("clusters.html")


@app.get("/api/wallet-clusters")
def wallet_clusters() -> object:
    window_seconds = _parse_cluster_window_seconds(request.args.get("window_seconds"))
    return jsonify(_wallet_clusters(get_storage(), window_seconds=window_seconds))


@app.get("/api/signal-quality")
def signal_quality() -> object:
    storage = get_storage()
    prediction_signal_rows = _fetch_signal_rows(storage, "whale_signals")
    perp_signal_rows = _fetch_signal_rows(storage, "perp_whale_signals")
    prediction_event_rows = _fetch_mark_rows(
        storage,
        "book_events",
        {(str(row["market_id"]), int(row["asset_id"])) for row in prediction_signal_rows},
    )
    perp_event_rows = _fetch_mark_rows(
        storage,
        "perp_book_events",
        {(str(row["market_id"]), int(row["asset_id"])) for row in perp_signal_rows},
    )
    prediction_quality = _signal_quality_breakdown(
        prediction_signal_rows,
        _build_event_series(prediction_event_rows),
        {"YES": 1, "NO": -1},
    )
    perp_quality = _signal_quality_breakdown(
        perp_signal_rows,
        _build_event_series(perp_event_rows),
        {"long": 1, "short": -1},
    )
    return jsonify(
        {
            "window_seconds": SIGNAL_QUALITY_WINDOW_SECONDS,
            "triggers": [
                {
                    "trigger_type": trigger_type,
                    "prediction": prediction_quality[trigger_type],
                    "perp": perp_quality[trigger_type],
                }
                for trigger_type in SIGNAL_QUALITY_TRIGGER_TYPES
            ],
        }
    )


@app.get("/api/decision-quality")
def decision_quality() -> object:
    return jsonify(_decision_quality_payload(get_storage()))


@app.get("/api/whale-spotlight")
def whale_spotlight() -> object:
    storage = get_storage()
    prediction_marks = _latest_prediction_marks(storage)
    perp_marks = _latest_perp_marks(storage)
    prediction_positions = _prediction_positions(storage, prediction_marks)
    perp_positions = _perp_positions(storage, perp_marks)
    return jsonify(_whale_spotlight(storage, prediction_positions, perp_positions))


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
