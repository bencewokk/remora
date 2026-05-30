from __future__ import annotations

import asyncio
import logging
import random
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
WALLET_STARTUP_INITIAL_ADDRESSES = 5
WALLET_STARTUP_POLL_SECONDS = 1.0
WALLET_REFRESH_DELAY_MIN_SECONDS = 0.3
WALLET_REFRESH_DELAY_MAX_SECONDS = 0.8
WALLET_REFRESH_MAX_ADDRESSES = 10


@dataclass(frozen=True)
class ResolvedWalletTrade:
    market_id: str
    pnl_usdh: float
    last_trade_ts: int


class WalletTrackerRateLimitSkip(Exception):
    def __init__(self, address: str) -> None:
        super().__init__(f"Wallet tracker rate limited for {address}")
        self.address = address


class WalletTracker:
    def __init__(self, config: Config, storage: Storage, adapter: HyperliquidAdapter) -> None:
        self.config = config
        self.storage = storage
        self.adapter = adapter
        self._stop_event = asyncio.Event()
        self._refresh_cursor = 0
        self._startup_refresh_completed = False
        self._startup_wait_logged = False

    async def run(self) -> None:
        while not self._stop_event.is_set():
            wait_seconds = self.config.wallet_refresh_seconds
            try:
                if not self._startup_refresh_completed:
                    self._startup_refresh_completed = await self._refresh_startup_addresses_once()
                    wait_seconds = (
                        self.config.wallet_refresh_seconds
                        if self._startup_refresh_completed
                        else WALLET_STARTUP_POLL_SECONDS
                    )
                else:
                    await self.refresh_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.warning("Wallet tracker refresh failed: %s", exc)
                if not self._startup_refresh_completed:
                    wait_seconds = WALLET_STARTUP_POLL_SECONDS
            if self._stop_event.is_set():
                break
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop_event.wait(), timeout=wait_seconds)

    async def stop(self) -> None:
        self._stop_event.set()

    async def refresh_once(self, addresses: list[str] | None = None, cycle_name: str = "scheduled") -> None:
        tracked_addresses = addresses if addresses is not None else self.storage.list_tracked_wallet_addresses()
        if not tracked_addresses:
            LOGGER.info("Wallet tracker has no tracked wallet addresses yet")
            return

        refresh_addresses = tracked_addresses if addresses is not None else self._select_addresses_for_refresh(tracked_addresses)
        if not refresh_addresses:
            return

        now_ts = int(datetime.now(tz=timezone.utc).timestamp())
        scores: list[WalletScore] = []
        scored_wallets = 0
        skipped_rate_limited = 0
        for index, address in enumerate(refresh_addresses):
            try:
                fills = await self._fetch_fills_for_address(address)
            except asyncio.CancelledError:
                raise
            except WalletTrackerRateLimitSkip as exc:
                skipped_rate_limited += 1
                LOGGER.warning("Wallet tracker rate limited for %s; skipping address for this cycle", exc.address)
                if index < len(refresh_addresses) - 1:
                    await self._pause(self._inter_wallet_delay_seconds())
                continue
            except Exception as exc:
                LOGGER.warning("Wallet tracker failed to refresh %s: %s", address, exc)
                if index < len(refresh_addresses) - 1:
                    await self._pause(self._inter_wallet_delay_seconds())
                continue

            resolved_trades = self._resolved_outcome_trades(fills)
            trade_count = len(resolved_trades)
            if trade_count == 0:
                if index < len(refresh_addresses) - 1:
                    await self._pause(self._inter_wallet_delay_seconds())
                continue

            win_count = sum(1 for trade in resolved_trades.values() if trade.pnl_usdh > 0)
            total_pnl = sum(trade.pnl_usdh for trade in resolved_trades.values())
            avg_pnl = total_pnl / trade_count if trade_count else 0.0
            win_rate = win_count / trade_count if trade_count else 0.0
            last_trade_ts = max((trade.last_trade_ts for trade in resolved_trades.values()), default=None)
            if last_trade_ts is not None:
                last_trade_ts = self._normalize_unix_timestamp(last_trade_ts)
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
            scored_wallets += 1

            if index < len(refresh_addresses) - 1:
                await self._pause(self._inter_wallet_delay_seconds())

        if scores:
            self.storage.upsert_wallet_scores(scores)

        LOGGER.info(
            "Wallet tracker %s refresh scored %d wallets and skipped %d due to 429",
            cycle_name,
            scored_wallets,
            skipped_rate_limited,
        )

    async def _refresh_startup_addresses_once(self) -> bool:
        addresses = self.storage.list_initial_tracked_wallet_addresses(limit=WALLET_STARTUP_INITIAL_ADDRESSES)
        if not addresses:
            if not self._startup_wait_logged:
                LOGGER.info("Wallet tracker startup waiting for tracked wallet addresses")
                self._startup_wait_logged = True
            return False

        self._startup_wait_logged = False
        await self.refresh_once(addresses=addresses, cycle_name="startup")
        return True

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

    async def _fetch_fills_for_address(self, address: str) -> list[dict]:
        try:
            return await self.adapter.fetch_user_fills_by_time(address)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if self._is_rate_limit_error(exc):
                raise WalletTrackerRateLimitSkip(address) from exc
            raise

    @staticmethod
    def _inter_wallet_delay_seconds() -> float:
        return random.uniform(WALLET_REFRESH_DELAY_MIN_SECONDS, WALLET_REFRESH_DELAY_MAX_SECONDS)

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