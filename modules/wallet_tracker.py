from __future__ import annotations

import asyncio
import logging
import re
from collections import defaultdict
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone

from config import Config
from .adapter import HyperliquidAdapter
from .models import WalletScore
from .storage import Storage

LOGGER = logging.getLogger(__name__)
OUTCOME_COIN_PATTERN = re.compile(r"^#\d+$")


@dataclass(frozen=True)
class ResolvedWalletTrade:
    market_id: str
    pnl_usdh: float
    last_trade_ts: int


class WalletTracker:
    def __init__(self, config: Config, storage: Storage, adapter: HyperliquidAdapter) -> None:
        self.config = config
        self.storage = storage
        self.adapter = adapter
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.refresh_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.warning("Wallet tracker refresh failed: %s", exc)
            if self._stop_event.is_set():
                break
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.config.wallet_refresh_seconds)

    async def stop(self) -> None:
        self._stop_event.set()

    async def refresh_once(self) -> None:
        addresses = self.storage.list_tracked_wallet_addresses()
        if not addresses:
            LOGGER.info("Wallet tracker has no tracked wallet addresses yet")
            return

        now_ts = int(datetime.now(tz=timezone.utc).timestamp())
        scores: list[WalletScore] = []
        for address in addresses:
            fills = await self.adapter.fetch_user_fills_by_time(address)
            resolved_trades = self._resolved_outcome_trades(fills)
            trade_count = len(resolved_trades)
            win_count = sum(1 for trade in resolved_trades.values() if trade.pnl_usdh > 0)
            total_pnl = sum(trade.pnl_usdh for trade in resolved_trades.values())
            avg_pnl = total_pnl / trade_count if trade_count else 0.0
            win_rate = win_count / trade_count if trade_count else 0.0
            last_trade_ts = max((trade.last_trade_ts for trade in resolved_trades.values()), default=None)
            scores.append(
                WalletScore(
                    address=address,
                    trade_count=trade_count,
                    win_count=win_count,
                    win_rate=win_rate,
                    total_pnl_usdh=total_pnl,
                    avg_pnl_per_trade=avg_pnl,
                    last_trade_ts=last_trade_ts,
                    last_updated_ts=now_ts,
                )
            )

        self.storage.upsert_wallet_scores(scores)

    @staticmethod
    def _resolved_outcome_trades(fills: list[dict]) -> dict[str, ResolvedWalletTrade]:
        grouped: dict[str, dict[str, float | int]] = defaultdict(lambda: {"pnl": 0.0, "last_trade_ts": 0})
        for fill in fills:
            coin = str(fill.get("coin", ""))
            if not OUTCOME_COIN_PATTERN.match(coin):
                continue
            market_id = WalletTracker._market_id_from_coin(coin)
            pnl = float(fill.get("closedPnl", 0.0) or 0.0)
            timestamp = WalletTracker._normalize_unix_timestamp(int(fill.get("time", 0) or 0))
            if pnl == 0.0:
                continue
            grouped[market_id]["pnl"] = float(grouped[market_id]["pnl"]) + pnl
            grouped[market_id]["last_trade_ts"] = max(int(grouped[market_id]["last_trade_ts"]), timestamp)

        return {
            market_id: ResolvedWalletTrade(
                market_id=market_id,
                pnl_usdh=float(values["pnl"]),
                last_trade_ts=int(values["last_trade_ts"]),
            )
            for market_id, values in grouped.items()
        }

    @staticmethod
    def _market_id_from_coin(coin: str) -> str:
        encoding = int(coin[1:])
        return str(encoding // 10)

    @staticmethod
    def _normalize_unix_timestamp(timestamp: int) -> int:
        if timestamp > 10_000_000_000:
            return timestamp // 1000
        return timestamp