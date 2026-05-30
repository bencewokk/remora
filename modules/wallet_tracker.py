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
WALLET_REFRESH_DELAY_SECONDS = 2.0
WALLET_REFRESH_MAX_ADDRESSES = 10
WALLET_REFRESH_BACKOFF_BASE_SECONDS = 2.0
WALLET_REFRESH_BACKOFF_CAP_SECONDS = 30.0
WALLET_REFRESH_MAX_RETRIES = 5


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
        self._refresh_cursor = 0

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

        refresh_addresses = self._select_addresses_for_refresh(addresses)
        if not refresh_addresses:
            return

        now_ts = int(datetime.now(tz=timezone.utc).timestamp())
        scores: list[WalletScore] = []
        for index, address in enumerate(refresh_addresses):
            try:
                fills = await self._fetch_fills_with_backoff(address)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.warning("Wallet tracker failed to refresh %s: %s", address, exc)
                if index < len(refresh_addresses) - 1:
                    await self._pause(WALLET_REFRESH_DELAY_SECONDS)
                continue

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

            if index < len(refresh_addresses) - 1:
                await self._pause(WALLET_REFRESH_DELAY_SECONDS)

        self.storage.upsert_wallet_scores(scores)

    def _select_addresses_for_refresh(self, addresses: list[str]) -> list[str]:
        unique_addresses = sorted({address for address in addresses if address}, key=str.lower)
        if not unique_addresses:
            return []
        if len(unique_addresses) <= WALLET_REFRESH_MAX_ADDRESSES:
            self._refresh_cursor = 0
            return unique_addresses

        start_index = self._refresh_cursor % len(unique_addresses)
        selected = [
            unique_addresses[(start_index + offset) % len(unique_addresses)]
            for offset in range(WALLET_REFRESH_MAX_ADDRESSES)
        ]
        self._refresh_cursor = (start_index + len(selected)) % len(unique_addresses)
        LOGGER.info(
            "Wallet tracker limiting refresh to %d of %d addresses this cycle",
            len(selected),
            len(unique_addresses),
        )
        return selected

    async def _fetch_fills_with_backoff(self, address: str) -> list[dict]:
        delay_seconds = WALLET_REFRESH_BACKOFF_BASE_SECONDS
        for attempt in range(1, WALLET_REFRESH_MAX_RETRIES + 1):
            try:
                return await self.adapter.fetch_user_fills_by_time(address)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not self._is_rate_limit_error(exc) or attempt == WALLET_REFRESH_MAX_RETRIES:
                    raise
                LOGGER.warning(
                    "Wallet tracker rate limited for %s on attempt %d/%d; retrying in %.1fs",
                    address,
                    attempt,
                    WALLET_REFRESH_MAX_RETRIES,
                    delay_seconds,
                )
                await self._pause(delay_seconds)
                delay_seconds = min(delay_seconds * 2, WALLET_REFRESH_BACKOFF_CAP_SECONDS)
        raise RuntimeError(f"Wallet tracker exhausted retries for {address}")

    async def _pause(self, seconds: float) -> None:
        if seconds <= 0 or self._stop_event.is_set():
            return
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)

    @staticmethod
    def _is_rate_limit_error(exc: Exception) -> bool:
        status = getattr(exc, "status", None)
        if status == 429:
            return True
        message = str(exc).lower()
        return "429" in message or "too many requests" in message

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