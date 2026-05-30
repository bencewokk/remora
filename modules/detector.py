from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Deque

from config import Config
from .models import OrderBookSnapshot, WhaleSignal
from .storage import Storage

MARKET_SIGNAL_COOLDOWN_SECONDS = 60


@dataclass(frozen=True)
class AggressionEvent:
    timestamp: datetime
    added_notional: float


class WhaleDetector:
    def __init__(
        self,
        config: Config,
        storage: Storage,
        snapshot_queue: asyncio.Queue[OrderBookSnapshot],
        signal_queue: asyncio.Queue[WhaleSignal],
    ) -> None:
        self.config = config
        self.storage = storage
        self.snapshot_queue = snapshot_queue
        self.signal_queue = signal_queue
        self._previous_snapshots: dict[int, OrderBookSnapshot] = {}
        self._aggression_history: dict[int, Deque[AggressionEvent]] = defaultdict(deque)
        self._mid_price_history: dict[int, Deque[tuple[datetime, float]]] = defaultdict(deque)
        self._market_cooldowns: dict[str, datetime] = {}
        self._active_market_id: str | None = None

    async def run(self) -> None:
        while True:
            snapshot = await self.snapshot_queue.get()
            try:
                signal = self.process_snapshot(snapshot)
                if signal is not None:
                    signal_id = self.storage.insert_whale_signal(signal)
                    await self.signal_queue.put(
                        WhaleSignal(
                            market_id=signal.market_id,
                            asset_id=signal.asset_id,
                            side=signal.side,
                            confidence=signal.confidence,
                            trigger_type=signal.trigger_type,
                            timestamp=signal.timestamp,
                            signal_id=signal_id,
                            wallet_bonus_applied=signal.wallet_bonus_applied,
                            details=signal.details,
                        )
                    )
            finally:
                self.snapshot_queue.task_done()

    def process_snapshot(self, snapshot: OrderBookSnapshot) -> WhaleSignal | None:
        self._reset_state_if_market_rotated(snapshot.market_id)
        previous = self._previous_snapshots.get(snapshot.asset_id)
        self._previous_snapshots[snapshot.asset_id] = snapshot

        if snapshot.is_reset or previous is None:
            self._record_mid_price(snapshot)
            return None

        top_of_book_added = self._top_of_book_added_notional(previous, snapshot)
        if top_of_book_added > 0:
            self._aggression_history[snapshot.asset_id].append(
                AggressionEvent(timestamp=snapshot.timestamp, added_notional=top_of_book_added)
            )

        self._trim_histories(snapshot)
        self._record_mid_price(snapshot)

        trigger_type = None
        details: dict[str, float | str] = {
            "top_of_book_added_usdh": round(top_of_book_added, 6),
            "recent_same_side_aggression_usdh": round(self._aggression_total(snapshot.asset_id, self.config.aggression_window_seconds), 6),
            "book_imbalance_ratio": round(self._book_imbalance_ratio(snapshot), 6),
        }
        if snapshot.mid_price is not None:
            details["mid_price"] = round(snapshot.mid_price, 6)
        if snapshot.trigger_address:
            details["trigger_address"] = snapshot.trigger_address

        if top_of_book_added >= self.config.whale_size_threshold:
            trigger_type = "single_step_liquidity"
        else:
            sequential_added = self._aggression_total(snapshot.asset_id, self.config.sequential_window_seconds)
            details["sequential_same_side_aggression_usdh"] = round(sequential_added, 6)
            if sequential_added >= self.config.whale_size_threshold:
                trigger_type = "rapid_sequential_aggression"
            else:
                price_impact = self._price_impact(snapshot.asset_id, snapshot.mid_price)
                details["price_impact"] = round(price_impact, 6)
                if (
                    price_impact >= self.config.price_impact_threshold
                    and sequential_added > 0
                ):
                    trigger_type = "price_impact_with_aggression"

        if trigger_type is None:
            return None

        if self._is_market_on_cooldown(snapshot.market_id, snapshot.timestamp):
            return None

        confidence, wallet_bonus_applied = self._confidence(trigger_type, snapshot)
        details["confidence"] = round(confidence, 6)
        details["wallet_bonus_applied"] = wallet_bonus_applied
        self._market_cooldowns[snapshot.market_id] = snapshot.timestamp + timedelta(seconds=MARKET_SIGNAL_COOLDOWN_SECONDS)
        return WhaleSignal(
            market_id=snapshot.market_id,
            asset_id=snapshot.asset_id,
            side=snapshot.outcome_side,
            confidence=confidence,
            trigger_type=trigger_type,
            timestamp=snapshot.timestamp,
            wallet_bonus_applied=wallet_bonus_applied,
            details=details,
        )

    def _reset_state_if_market_rotated(self, market_id: str) -> None:
        if self._active_market_id is None:
            self._active_market_id = market_id
            return
        if self._active_market_id == market_id:
            return
        self._active_market_id = market_id
        self._previous_snapshots.clear()
        self._aggression_history.clear()
        self._mid_price_history.clear()
        self._market_cooldowns.clear()

    def _is_market_on_cooldown(self, market_id: str, timestamp: datetime) -> bool:
        cooldown_until = self._market_cooldowns.get(market_id)
        if cooldown_until is None:
            return False
        if timestamp >= cooldown_until:
            del self._market_cooldowns[market_id]
            return False
        return True

    def _record_mid_price(self, snapshot: OrderBookSnapshot) -> None:
        if snapshot.mid_price is None:
            return
        self._mid_price_history[snapshot.asset_id].append((snapshot.timestamp, snapshot.mid_price))

    def _trim_histories(self, snapshot: OrderBookSnapshot) -> None:
        aggression_cutoff = snapshot.timestamp - timedelta(seconds=self.config.aggression_window_seconds)
        aggression_history = self._aggression_history[snapshot.asset_id]
        while aggression_history and aggression_history[0].timestamp < aggression_cutoff:
            aggression_history.popleft()

        price_cutoff = snapshot.timestamp - timedelta(seconds=self.config.price_impact_window_seconds)
        price_history = self._mid_price_history[snapshot.asset_id]
        while price_history and price_history[0][0] < price_cutoff:
            price_history.popleft()

    @staticmethod
    def _size_at_price(levels: tuple, price: float) -> float:
        for level in levels:
            if level.price == price:
                return level.size
        return 0.0

    def _top_of_book_added_notional(self, previous: OrderBookSnapshot, current: OrderBookSnapshot) -> float:
        if not current.bids:
            return 0.0
        current_bid = current.bids[0]
        previous_size = self._size_at_price(previous.bids, current_bid.price)
        added_size = max(current_bid.size - previous_size, 0.0)
        return current_bid.price * added_size

    def _aggression_total(self, asset_id: int, window_seconds: int) -> float:
        history = self._aggression_history[asset_id]
        if not history:
            return 0.0
        cutoff = history[-1].timestamp - timedelta(seconds=window_seconds)
        return sum(event.added_notional for event in history if event.timestamp >= cutoff)

    def _book_imbalance_ratio(self, snapshot: OrderBookSnapshot) -> float:
        bid_notional = sum(level.price * level.size for level in snapshot.bids[: self.config.book_imbalance_levels])
        ask_notional = sum(level.price * level.size for level in snapshot.asks[: self.config.book_imbalance_levels])
        total = bid_notional + ask_notional
        if total == 0:
            return 0.5
        return bid_notional / total

    def _price_impact(self, asset_id: int, current_mid: float | None) -> float:
        if current_mid is None:
            return 0.0
        history = self._mid_price_history[asset_id]
        if not history:
            return 0.0
        baseline_mid = history[0][1]
        if baseline_mid == 0:
            return 0.0
        return max((current_mid - baseline_mid) / baseline_mid, 0.0)

    def _confidence(self, trigger_type: str, snapshot: OrderBookSnapshot) -> tuple[float, bool]:
        base_scores = {
            "single_step_liquidity": 0.6,
            "rapid_sequential_aggression": 0.7,
            "price_impact_with_aggression": 0.8,
        }
        confidence = base_scores[trigger_type]
        wallet_bonus_applied = False

        if self._aggression_total(snapshot.asset_id, self.config.aggression_window_seconds) >= self.config.whale_size_threshold:
            confidence += 0.1
        if self._book_imbalance_ratio(snapshot) >= self.config.book_imbalance_ratio_threshold:
            confidence += 0.1
        if snapshot.trigger_address:
            wallet_score = self.storage.get_wallet_score(snapshot.trigger_address)
            if wallet_score is not None and wallet_score.win_rate >= 0.65 and wallet_score.trade_count >= 8:
                confidence += 0.15
                wallet_bonus_applied = True
        return min(confidence, 1.0), wallet_bonus_applied
