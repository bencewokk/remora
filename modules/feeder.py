from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

import websockets

from config import Config
from .models import DepthLevel, MarketInfo, OrderBookSnapshot
from .storage import Storage

LOGGER = logging.getLogger(__name__)


class L2BookFeeder:
    def __init__(
        self,
        config: Config,
        storage: Storage,
        queue: asyncio.Queue[OrderBookSnapshot],
        market: MarketInfo,
    ) -> None:
        self.config = config
        self.storage = storage
        self.queue = queue
        self.market = market
        self._stop_event = asyncio.Event()
        self._last_fingerprints: dict[str, tuple] = {}

    async def run(self) -> None:
        backoff_seconds = 1
        while not self._stop_event.is_set():
            try:
                await self._stream_once()
                backoff_seconds = 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.warning("Feeder disconnected: %s", exc)
                await asyncio.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 30)

    async def stop(self) -> None:
        self._stop_event.set()

    async def _stream_once(self) -> None:
        subscriptions = (self.market.yes_coin, self.market.no_coin)
        async with websockets.connect(self.config.ws_url, ping_interval=20, ping_timeout=20) as websocket:
            for coin in subscriptions:
                await websocket.send(
                    json.dumps(
                        {
                            "method": "subscribe",
                            "subscription": {"type": "l2Book", "coin": coin},
                        }
                    )
                )

            reset_pending = {coin: True for coin in subscriptions}
            while not self._stop_event.is_set():
                message = json.loads(await websocket.recv())
                if message.get("channel") != "l2Book":
                    continue
                payload = message.get("data", {})
                coin = str(payload.get("coin", ""))
                if coin not in reset_pending:
                    continue
                snapshot = self._normalize_snapshot(payload, coin, reset_pending[coin])
                reset_pending[coin] = False
                if snapshot is None:
                    continue
                self.storage.insert_book_event(snapshot)
                await self.queue.put(snapshot)

    def _normalize_snapshot(
        self,
        payload: dict,
        coin: str,
        is_reset: bool,
    ) -> OrderBookSnapshot | None:
        levels = payload.get("levels", [[], []])
        bids = tuple(self._parse_levels(levels[0]))
        asks = tuple(self._parse_levels(levels[1]))
        fingerprint = (
            tuple((level.price, level.size) for level in bids),
            tuple((level.price, level.size) for level in asks),
            payload.get("time"),
        )
        if self._last_fingerprints.get(coin) == fingerprint:
            return None
        self._last_fingerprints[coin] = fingerprint

        best_bid = bids[0].price if bids else None
        best_ask = asks[0].price if asks else None
        mid_price = None
        if best_bid is not None and best_ask is not None:
            mid_price = (best_bid + best_ask) / 2

        asset_id = self.market.yes_asset_id if coin == self.market.yes_coin else self.market.no_asset_id
        outcome_side = "YES" if coin == self.market.yes_coin else "NO"
        timestamp_ms = payload.get("time")
        timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc) if timestamp_ms else datetime.now(tz=timezone.utc)
        return OrderBookSnapshot(
            market_id=self.market.market_id,
            asset_id=asset_id,
            outcome_side=outcome_side,
            bids=bids,
            asks=asks,
            best_bid=best_bid,
            best_ask=best_ask,
            mid_price=mid_price,
            timestamp=timestamp,
            sequence_id=timestamp_ms,
            is_reset=is_reset,
            raw_message=payload,
        )

    @staticmethod
    def _parse_levels(levels: list[dict]) -> list[DepthLevel]:
        return [DepthLevel(price=float(level["px"]), size=float(level["sz"])) for level in levels]
