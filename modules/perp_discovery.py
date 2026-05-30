from __future__ import annotations

import asyncio
import logging
from contextlib import suppress

from config import Config
from .adapter import HyperliquidAdapter
from .models import PerpMarket

LOGGER = logging.getLogger(__name__)
TRACKED_PERP_COINS = ("BTC", "ETH", "SOL", "HYPE", "ARB", "OP")


def _ctx_float(ctx: dict, key: str) -> float:
    value = ctx.get(key)
    if value in (None, ""):
        return 0.0
    return float(value)


async def discover_perp_markets(
    adapter: HyperliquidAdapter,
    tracked_coins: tuple[str, ...] = TRACKED_PERP_COINS,
) -> list[PerpMarket]:
    meta, asset_ctxs = await adapter.fetch_perp_meta_and_asset_ctxs()
    tracked = set(tracked_coins)
    markets: list[PerpMarket] = []
    for asset_id, universe_item in enumerate(meta.get("universe", [])):
        coin = str(universe_item.get("name", ""))
        if coin not in tracked:
            continue
        ctx = asset_ctxs[asset_id] if asset_id < len(asset_ctxs) else {}
        mark_price = ctx.get("markPx")
        markets.append(
            PerpMarket(
                market_id=f"perp:{coin}",
                coin=coin,
                asset_id=asset_id,
                mark_price=float(mark_price) if mark_price not in (None, "") else None,
                funding_rate=_ctx_float(ctx, "funding"),
                open_interest=_ctx_float(ctx, "openInterest"),
            )
        )
    return markets


class PerpDiscoveryService:
    def __init__(
        self,
        config: Config,
        adapter: HyperliquidAdapter,
        tracked_coins: tuple[str, ...] = TRACKED_PERP_COINS,
    ) -> None:
        self.config = config
        self.adapter = adapter
        self.tracked_coins = tracked_coins
        self._markets: dict[str, PerpMarket] = {}
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.refresh_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.warning("Perp discovery refresh failed: %s", exc)
            if self._stop_event.is_set():
                break
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.config.discovery_interval_seconds)

    async def stop(self) -> None:
        self._stop_event.set()

    async def refresh_once(self) -> list[PerpMarket]:
        markets = await discover_perp_markets(self.adapter, self.tracked_coins)
        self._markets = {market.coin: market for market in markets}
        return markets

    def list_markets(self) -> list[PerpMarket]:
        return [self._markets[coin] for coin in self.tracked_coins if coin in self._markets]

    def get_market(self, coin: str) -> PerpMarket | None:
        return self._markets.get(coin)