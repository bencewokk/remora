from __future__ import annotations

import asyncio
import logging
import math
import random
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone

from config import Config
from .adapter import HyperliquidAdapter
from .models import WalletScore
from .storage import Storage

LOGGER = logging.getLogger(__name__)
WALLET_STARTUP_POLL_SECONDS = 1.0
WALLET_REFRESH_DELAY_MIN_SECONDS = 0.3
WALLET_REFRESH_DELAY_MAX_SECONDS = 0.8
WALLET_REFRESH_MAX_ADDRESSES = 50
WALLET_STARTUP_INITIAL_ADDRESSES = WALLET_REFRESH_MAX_ADDRESSES


@dataclass(frozen=True)
class ScoredWalletFill:
    pnl_usdh: float
    trade_ts: int


@dataclass(frozen=True)
class WalletRefreshPlan:
    addresses: list[str]
    cycle_number: int
    total_cycles: int
    remaining_unscored_before: int
    selected_unscored_keys: frozenset[str]


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
        self._startup_refresh_completed = False
        self._startup_wait_logged = False
        self._scheduled_cycle_counter = 0

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

        refresh_plan = None if addresses is not None else self._select_addresses_for_refresh(tracked_addresses)
        refresh_addresses = tracked_addresses if refresh_plan is None else refresh_plan.addresses
        if not refresh_addresses:
            return

        if refresh_plan is None:
            LOGGER.info(
                "Wallet tracker %s refresh starting for %d wallets",
                cycle_name,
                len(refresh_addresses),
            )
        else:
            LOGGER.info(
                "Wallet tracker cycle %d/%d starting: refreshing %d wallets, %d currently unscored",
                refresh_plan.cycle_number,
                refresh_plan.total_cycles,
                len(refresh_addresses),
                refresh_plan.remaining_unscored_before,
            )

        now_ts = int(datetime.now(tz=timezone.utc).timestamp())
        scores: list[WalletScore] = []
        scored_wallets = 0
        skipped_rate_limited = 0
        newly_scored_unscored_wallets = 0
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

            scored_fills = self._scored_fills(fills)
            trade_count = len(scored_fills)
            if trade_count == 0:
                if index < len(refresh_addresses) - 1:
                    await self._pause(self._inter_wallet_delay_seconds())
                continue

            win_count = sum(1 for fill in scored_fills if fill.pnl_usdh > 0)
            total_pnl = sum(fill.pnl_usdh for fill in scored_fills)
            avg_pnl = total_pnl / trade_count if trade_count else 0.0
            win_rate = win_count / trade_count if trade_count else 0.0
            last_trade_ts = max((fill.trade_ts for fill in scored_fills), default=None)
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
            if refresh_plan is not None and address.lower() in refresh_plan.selected_unscored_keys:
                newly_scored_unscored_wallets += 1

            if index < len(refresh_addresses) - 1:
                await self._pause(self._inter_wallet_delay_seconds())

        if scores:
            self.storage.upsert_wallet_scores(scores)

        if refresh_plan is None:
            LOGGER.info(
                "Wallet tracker %s refresh scored %d wallets and skipped %d due to 429",
                cycle_name,
                scored_wallets,
                skipped_rate_limited,
            )
            return

        remaining_unscored = max(refresh_plan.remaining_unscored_before - newly_scored_unscored_wallets, 0)
        LOGGER.info(
            "Wallet tracker cycle %d/%d: scored %d wallets, %d remaining unscored",
            refresh_plan.cycle_number,
            refresh_plan.total_cycles,
            scored_wallets,
            remaining_unscored,
        )
        if skipped_rate_limited > 0:
            LOGGER.info(
                "Wallet tracker cycle %d/%d skipped %d wallets due to 429",
                refresh_plan.cycle_number,
                refresh_plan.total_cycles,
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

    def _select_addresses_for_refresh(self, addresses: list[str]) -> WalletRefreshPlan:
        unique_addresses_by_key: dict[str, str] = {}
        for address in addresses:
            normalized_address = address.strip()
            if not normalized_address:
                continue
            unique_addresses_by_key.setdefault(normalized_address.lower(), normalized_address)
        if not unique_addresses_by_key:
            return WalletRefreshPlan([], 1, 1, 0, frozenset())

        scores_by_key = {
            str(row["address"]).lower(): row
            for row in self.storage.list_wallet_scores()
        }
        unscored_addresses = [
            unique_addresses_by_key[address_key]
            for address_key in sorted(unique_addresses_by_key)
            if address_key not in scores_by_key
        ]
        scored_addresses = sorted(
            [
                {
                    "address": unique_addresses_by_key[address_key],
                    "address_key": address_key,
                    "last_updated_ts": int(scores_by_key[address_key]["last_updated_ts"]),
                }
                for address_key in unique_addresses_by_key
                if address_key in scores_by_key
            ],
            key=lambda item: (item["last_updated_ts"], item["address"].lower()),
        )
        prioritized_addresses = unscored_addresses + [item["address"] for item in scored_addresses]
        selected = prioritized_addresses[:WALLET_REFRESH_MAX_ADDRESSES]
        total_cycles = max(math.ceil(len(prioritized_addresses) / WALLET_REFRESH_MAX_ADDRESSES), 1)
        self._scheduled_cycle_counter += 1
        cycle_number = ((self._scheduled_cycle_counter - 1) % total_cycles) + 1

        if len(prioritized_addresses) > WALLET_REFRESH_MAX_ADDRESSES:
            LOGGER.info(
                "Wallet tracker limiting refresh to %d of %d addresses this cycle",
                len(selected),
                len(prioritized_addresses),
            )

        return WalletRefreshPlan(
            addresses=selected,
            cycle_number=cycle_number,
            total_cycles=total_cycles,
            remaining_unscored_before=len(unscored_addresses),
            selected_unscored_keys=frozenset(address.lower() for address in selected if address.lower() not in scores_by_key),
        )

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
    def _scored_fills(fills: list[dict]) -> list[ScoredWalletFill]:
        scored_fills: list[ScoredWalletFill] = []
        for fill in fills:
            pnl = float(fill.get("closedPnl", 0.0) or 0.0)
            if pnl == 0.0:
                continue
            timestamp = WalletTracker._normalize_unix_timestamp(int(fill.get("time", 0) or 0))
            scored_fills.append(
                ScoredWalletFill(
                    pnl_usdh=pnl,
                    trade_ts=timestamp,
                )
            )
        return scored_fills

    @staticmethod
    def _normalize_unix_timestamp(timestamp: int) -> int:
        if timestamp > 10_000_000_000:
            return timestamp // 1000
        return timestamp