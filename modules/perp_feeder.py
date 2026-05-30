from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from datetime import datetime, timezone
from typing import Deque

import websockets

from config import Config
from .models import DepthLevel, PerpMarket, PerpSnapshot
from .perp_discovery import PerpDiscoveryService
from .storage import Storage

LOGGER = logging.getLogger(__name__)


class PerpFeeder:
    def __init__(
        self,
        config: Config,
        storage: Storage,
        queue: asyncio.Queue[PerpSnapshot],
        discovery: PerpDiscoveryService,
    ) -> None:
        self.config = config
        self.storage = storage
        self.queue = queue
        self.discovery = discovery
        self._stop_event = asyncio.Event()
        self._markets_by_coin: dict[str, PerpMarket] = {}
        self._last_fingerprints: dict[str, tuple] = {}
        self._recent_trade_addresses: dict[str, Deque[tuple[datetime, str]]] = {}
        self._latest_books: dict[str, dict] = {}
        self._latest_ctx: dict[str, dict] = {}
        self._latest_oi_change_pct: dict[str, float] = {}
        self._last_open_interest: dict[str, float] = {}

    async def run(self) -> None:
        backoff_seconds = 1
        while not self._stop_event.is_set():
            try:
                await self._stream_once()
                backoff_seconds = 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.warning("Perp feeder disconnected: %s", exc)
                await asyncio.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 30)

    async def stop(self) -> None:
        self._stop_event.set()

    async def _stream_once(self) -> None:
        await self._refresh_markets()
        if not self._markets_by_coin:
            await asyncio.sleep(5)
            return

        coins = tuple(self._markets_by_coin)
        async with websockets.connect(self.config.ws_url, ping_interval=20, ping_timeout=20) as websocket:
            for coin in coins:
                for subscription_type in ("l2Book", "trades", "activeAssetCtx"):
                    await websocket.send(
                        json.dumps(
                            {
                                "method": "subscribe",
                                "subscription": {"type": subscription_type, "coin": coin},
                            }
                        )
                    )

            reset_pending = {coin: True for coin in coins}
            while not self._stop_event.is_set():
                message = json.loads(await websocket.recv())
                channel = message.get("channel")
                payload = message.get("data", {})
                if channel == "trades":
                    self._record_trade_addresses(payload)
                    continue
                if channel == "activeAssetCtx":
                    snapshot = self._handle_active_asset_ctx(payload)
                elif channel == "l2Book":
                    coin = str(payload.get("coin", ""))
                    if coin not in reset_pending:
                        continue
                    snapshot = self._handle_l2_book(payload, reset_pending[coin])
                    reset_pending[coin] = False
                else:
                    continue

                if snapshot is None:
                    continue
                self.storage.insert_perp_book_event(snapshot)
                await self.queue.put(snapshot)

    async def _refresh_markets(self) -> None:
        markets = self.discovery.list_markets()
        if not markets:
            markets = await self.discovery.refresh_once()
        self._markets_by_coin = {market.coin: market for market in markets}
        for market in markets:
            self._recent_trade_addresses.setdefault(market.coin, deque())
            self._latest_ctx.setdefault(
                market.coin,
                {
                    "funding": market.funding_rate,
                    "openInterest": market.open_interest,
                    "markPx": market.mark_price,
                },
            )
            self._last_open_interest.setdefault(market.coin, market.open_interest)
            self._latest_oi_change_pct.setdefault(market.coin, 0.0)

    def _handle_l2_book(self, payload: dict, is_reset: bool) -> PerpSnapshot | None:
        coin = str(payload.get("coin", ""))
        market = self._markets_by_coin.get(coin)
        if market is None:
            return None
        levels = payload.get("levels", [[], []])
        bids = tuple(self._parse_levels(levels[0]))
        asks = tuple(self._parse_levels(levels[1]))
        best_bid = bids[0].price if bids else None
        best_ask = asks[0].price if asks else None
        mid_price = (best_bid + best_ask) / 2 if best_bid is not None and best_ask is not None else None
        timestamp_ms = payload.get("time")
        timestamp = self._timestamp_from_ms(timestamp_ms)
        self._latest_books[coin] = {
            "timestamp": timestamp,
            "sequence_id": int(timestamp_ms) if isinstance(timestamp_ms, (int, float)) else None,
            "bids": bids,
            "asks": asks,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid_price": mid_price,
            "is_reset": is_reset,
            "raw_message": payload,
        }
        return self._build_snapshot(coin, payload, is_reset=is_reset)

    def _handle_active_asset_ctx(self, payload: dict) -> PerpSnapshot | None:
        coin = str(payload.get("coin", ""))
        if coin not in self._markets_by_coin:
            return None
        ctx = payload.get("ctx") if isinstance(payload.get("ctx"), dict) else payload
        timestamp = self._timestamp_from_ms(payload.get("time"))
        previous_oi = self._last_open_interest.get(coin)
        open_interest = self._ctx_float(ctx, "openInterest")
        if previous_oi and previous_oi > 0:
            self._latest_oi_change_pct[coin] = (open_interest - previous_oi) / previous_oi
        else:
            self._latest_oi_change_pct[coin] = 0.0
        if open_interest > 0:
            self._last_open_interest[coin] = open_interest
        self._latest_ctx[coin] = ctx
        if coin not in self._latest_books:
            return None
        return self._build_snapshot(coin, payload, is_reset=False, timestamp=timestamp)

    def _build_snapshot(
        self,
        coin: str,
        raw_message: dict,
        is_reset: bool,
        timestamp: datetime | None = None,
    ) -> PerpSnapshot | None:
        market = self._markets_by_coin.get(coin)
        book = self._latest_books.get(coin)
        if market is None or book is None:
            return None
        ctx = self._latest_ctx.get(coin, {})
        funding_rate = self._ctx_float(ctx, "funding")
        open_interest = self._ctx_float(ctx, "openInterest")
        fingerprint = (
            tuple((level.price, level.size) for level in book["bids"]),
            tuple((level.price, level.size) for level in book["asks"]),
            round(funding_rate, 10),
            round(open_interest, 6),
            round(self._latest_oi_change_pct.get(coin, 0.0), 10),
            book["sequence_id"],
            is_reset,
        )
        if self._last_fingerprints.get(coin) == fingerprint:
            return None
        self._last_fingerprints[coin] = fingerprint
        snapshot_timestamp = timestamp or book["timestamp"]
        trigger_address = self._recent_trigger_address(coin, snapshot_timestamp)
        return PerpSnapshot(
            market_id=market.market_id,
            coin=coin,
            asset_id=market.asset_id,
            timestamp=snapshot_timestamp,
            sequence_id=book["sequence_id"],
            bids=book["bids"],
            asks=book["asks"],
            best_bid=book["best_bid"],
            best_ask=book["best_ask"],
            mid_price=book["mid_price"],
            funding_rate=funding_rate,
            open_interest=open_interest,
            oi_change_pct=self._latest_oi_change_pct.get(coin, 0.0),
            is_reset=is_reset,
            trigger_address=trigger_address,
            raw_message=raw_message,
        )

    @staticmethod
    def _parse_levels(levels: list[dict]) -> list[DepthLevel]:
        return [DepthLevel(price=float(level["px"]), size=float(level["sz"])) for level in levels]

    def _record_trade_addresses(self, trades_payload: list[dict]) -> None:
        for trade in trades_payload:
            coin = str(trade.get("coin", ""))
            if coin not in self._recent_trade_addresses:
                continue
            users = trade.get("users") or []
            if len(users) != 2:
                continue
            side = str(trade.get("side", ""))
            trigger_address = str(users[0]) if side in {"B", "BUY", "Bid"} else str(users[1])
            timestamp = self._timestamp_from_ms(trade.get("time"))
            queue = self._recent_trade_addresses[coin]
            queue.append((timestamp, trigger_address))
            cutoff = timestamp.timestamp() - 5
            while queue and queue[0][0].timestamp() < cutoff:
                queue.popleft()

    def _recent_trigger_address(self, coin: str, timestamp: datetime) -> str | None:
        trade_history = self._recent_trade_addresses.get(coin)
        if not trade_history:
            return None
        cutoff = timestamp.timestamp() - 5
        while trade_history and trade_history[0][0].timestamp() < cutoff:
            trade_history.popleft()
        if not trade_history:
            return None
        return trade_history[-1][1]

    @staticmethod
    def _timestamp_from_ms(timestamp_ms: object) -> datetime:
        if isinstance(timestamp_ms, (int, float)):
            return datetime.fromtimestamp(float(timestamp_ms) / 1000, tz=timezone.utc)
        return datetime.now(tz=timezone.utc)

    @staticmethod
    def _ctx_float(ctx: dict, key: str) -> float:
        value = ctx.get(key)
        if value in (None, ""):
            return 0.0
        return float(value)