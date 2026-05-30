from __future__ import annotations

import asyncio
import logging
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Deque

from config import Config
from .models import PerpSnapshot, PerpWhaleSignal
from .storage import Storage

LOGGER = logging.getLogger(__name__)
MARKET_SIGNAL_COOLDOWN_SECONDS = 60
OI_SPIKE_WINDOW_SECONDS = 60
POST_RESET_WARMUP_SNAPSHOTS = 3


@dataclass(frozen=True)
class AggressionEvent:
    timestamp: datetime
    added_notional: float
    side: str


class PerpWhaleDetector:
    def __init__(
        self,
        config: Config,
        storage: Storage,
        snapshot_queue: asyncio.Queue[PerpSnapshot],
        signal_queue: asyncio.Queue[PerpWhaleSignal],
    ) -> None:
        self.config = config
        self.storage = storage
        self.snapshot_queue = snapshot_queue
        self.signal_queue = signal_queue
        self.whale_threshold = config.perp_whale_size_threshold
        self.price_impact_threshold = config.perp_price_impact_threshold
        self.min_trigger_delta_usdh = config.perp_min_trigger_delta_usdh
        self.min_signal_confidence = config.perp_min_signal_confidence
        self.oi_spike_threshold_pct = config.perp_oi_spike_threshold_pct
        self.funding_divergence_threshold = config.funding_divergence_threshold
        self.book_imbalance_ratio_threshold = config.book_imbalance_ratio_threshold
        self._previous_snapshots: dict[int, PerpSnapshot] = {}
        self._aggression_history: dict[int, Deque[AggressionEvent]] = defaultdict(deque)
        self._mid_price_history: dict[int, Deque[tuple[datetime, float]]] = defaultdict(deque)
        self._open_interest_history: dict[int, Deque[tuple[datetime, float]]] = defaultdict(deque)
        self._market_cooldowns: dict[tuple[str, str], datetime] = {}
        self._warmup_remaining: dict[int, int] = {}
        LOGGER.info(
            "PerpWhaleDetector initialized with whale threshold: %s, min trigger delta: %s, min signal confidence: %s",
            self.whale_threshold,
            self.min_trigger_delta_usdh,
            self.min_signal_confidence,
        )

    async def run(self) -> None:
        while True:
            snapshot = await self.snapshot_queue.get()
            try:
                signal = self.process_snapshot(snapshot)
                if signal is not None:
                    signal_id = self.storage.insert_perp_whale_signal(signal)
                    await self.signal_queue.put(
                        PerpWhaleSignal(
                            market_id=signal.market_id,
                            coin=signal.coin,
                            asset_id=signal.asset_id,
                            side=signal.side,
                            confidence=signal.confidence,
                            trigger_type=signal.trigger_type,
                            timestamp=signal.timestamp,
                            signal_id=signal_id,
                            wallet_bonus_applied=signal.wallet_bonus_applied,
                            trigger_oi_spike=signal.trigger_oi_spike,
                            trigger_funding=signal.trigger_funding,
                            details=signal.details,
                        )
                    )
            finally:
                self.snapshot_queue.task_done()

    def process_snapshot(self, snapshot: PerpSnapshot) -> PerpWhaleSignal | None:
        if snapshot.is_reset:
            self._warmup_remaining[snapshot.asset_id] = POST_RESET_WARMUP_SNAPSHOTS
            self._baseline_snapshot(snapshot)
            return None

        warmup_remaining = self._warmup_remaining.get(snapshot.asset_id, 0)
        if warmup_remaining > 0:
            self._warmup_remaining[snapshot.asset_id] = warmup_remaining - 1
            self._baseline_snapshot(snapshot)
            return None

        previous = self._previous_snapshots.get(snapshot.asset_id)
        self._previous_snapshots[snapshot.asset_id] = snapshot
        self._record_mid_price(snapshot)
        self._record_open_interest(snapshot)

        if previous is None:
            return None

        long_added, short_added = self._top_of_book_added_notional(previous, snapshot)
        if long_added > 0:
            self._aggression_history[snapshot.asset_id].append(
                AggressionEvent(timestamp=snapshot.timestamp, added_notional=long_added, side="long")
            )
        if short_added > 0:
            self._aggression_history[snapshot.asset_id].append(
                AggressionEvent(timestamp=snapshot.timestamp, added_notional=short_added, side="short")
            )

        self._trim_histories(snapshot)
        long_aggression = self._aggression_total(snapshot.asset_id, "long", self.config.sequential_window_seconds)
        short_aggression = self._aggression_total(snapshot.asset_id, "short", self.config.sequential_window_seconds)
        price_impact = self._price_impact(snapshot.asset_id, snapshot.mid_price)
        oi_change_pct = self._open_interest_change(snapshot.asset_id)
        imbalance = self._book_imbalance_ratio(snapshot)

        triggers: list[str] = []
        trigger_sides: list[str] = []
        details: dict[str, float | str | bool] = {
            "top_bid_added_usdh": round(long_added, 6),
            "top_ask_added_usdh": round(short_added, 6),
            "recent_long_aggression_usdh": round(long_aggression, 6),
            "recent_short_aggression_usdh": round(short_aggression, 6),
            "book_imbalance_ratio": round(imbalance, 6),
            "price_impact": round(price_impact, 6),
            "funding_rate": round(snapshot.funding_rate, 8),
            "open_interest": round(snapshot.open_interest, 6),
            "oi_change_pct": round(oi_change_pct, 6),
        }
        if snapshot.mid_price is not None:
            details["mid_price"] = round(snapshot.mid_price, 6)
        if snapshot.trigger_address:
            details["trigger_address"] = snapshot.trigger_address

        single_step_side = self._single_step_trigger_side(
            long_added=long_added,
            short_added=short_added,
            imbalance=imbalance,
        )
        if single_step_side is not None:
            triggers.append("single_step_liquidity")
            trigger_sides.append(single_step_side)

        rapid_trigger_side = self._rapid_trigger_side(
            long_added=long_added,
            short_added=short_added,
            long_aggression=long_aggression,
            short_aggression=short_aggression,
        )
        if rapid_trigger_side is not None:
            triggers.append("rapid_sequential_aggression")
            trigger_sides.append(rapid_trigger_side)

        if abs(price_impact) >= self.price_impact_threshold:
            triggers.append("price_impact")
            trigger_sides.append("long" if price_impact > 0 else "short")

        trigger_oi_spike = abs(oi_change_pct) >= self.oi_spike_threshold_pct
        if trigger_oi_spike:
            triggers.append("oi_spike")
            trigger_sides.append(self._direction_from_context(price_impact, imbalance))

        trigger_funding = False
        if abs(snapshot.funding_rate) >= self.funding_divergence_threshold:
            funding_side = "long" if snapshot.funding_rate > 0 else "short"
            if (funding_side == "long" and price_impact > 0) or (funding_side == "short" and price_impact < 0):
                trigger_funding = True
                triggers.append("funding_divergence")
                trigger_sides.append(funding_side)

        if not triggers:
            return None

        if "rapid_sequential_aggression" in triggers and long_added <= 0 and short_added <= 0:
            return None

        side = self._resolve_side(trigger_sides)
        if self._is_market_side_on_cooldown(snapshot.market_id, side, snapshot.timestamp):
            return None

        triggering_same_side_delta = long_added if side == "long" else short_added
        triggering_same_side_cumulative = long_aggression if side == "long" else short_aggression

        confidence, wallet_bonus_applied = self._confidence(
            snapshot=snapshot,
            triggers=triggers,
            side=side,
            price_impact=price_impact,
            trigger_oi_spike=trigger_oi_spike,
        )
        if confidence < self.min_signal_confidence:
            return None
        details["confidence"] = round(confidence, 6)
        details["wallet_bonus_applied"] = wallet_bonus_applied
        details["trigger_oi_spike"] = trigger_oi_spike
        details["trigger_funding"] = trigger_funding
        details["triggering_bid_added_usdh"] = round(long_added, 6)
        details["triggering_ask_added_usdh"] = round(short_added, 6)
        details["triggering_same_side_delta_usdh"] = round(triggering_same_side_delta, 6)
        details["triggering_same_side_cumulative_usdh"] = round(triggering_same_side_cumulative, 6)

        self._rearm_signal_state(snapshot)
        self._set_market_cooldown(snapshot.market_id, snapshot.timestamp)
        return PerpWhaleSignal(
            market_id=snapshot.market_id,
            coin=snapshot.coin,
            asset_id=snapshot.asset_id,
            side=side,
            confidence=confidence,
            trigger_type="+".join(triggers),
            timestamp=snapshot.timestamp,
            wallet_bonus_applied=wallet_bonus_applied,
            trigger_oi_spike=trigger_oi_spike,
            trigger_funding=trigger_funding,
            details=details,
        )

    def _reset_asset_state(self, asset_id: int) -> None:
        self._previous_snapshots.pop(asset_id, None)
        self._aggression_history.pop(asset_id, None)
        self._mid_price_history.pop(asset_id, None)
        self._open_interest_history.pop(asset_id, None)

    def _baseline_snapshot(self, snapshot: PerpSnapshot) -> None:
        self._reset_asset_state(snapshot.asset_id)
        self._previous_snapshots[snapshot.asset_id] = snapshot
        self._record_mid_price(snapshot)
        self._record_open_interest(snapshot)

    def _rearm_signal_state(self, snapshot: PerpSnapshot) -> None:
        self._reset_asset_state(snapshot.asset_id)

    def _rapid_trigger_side(
        self,
        *,
        long_added: float,
        short_added: float,
        long_aggression: float,
        short_aggression: float,
    ) -> str | None:
        long_eligible = (
            long_aggression >= self.whale_threshold
            and long_added >= self.min_trigger_delta_usdh
        )
        short_eligible = (
            short_aggression >= self.whale_threshold
            and short_added >= self.min_trigger_delta_usdh
        )
        if long_eligible and short_eligible:
            return "long" if long_aggression >= short_aggression else "short"
        if long_eligible:
            return "long"
        if short_eligible:
            return "short"
        return None

    def _single_step_trigger_side(
        self,
        *,
        long_added: float,
        short_added: float,
        imbalance: float,
    ) -> str | None:
        long_eligible = (
            long_added >= self.whale_threshold
            and imbalance >= self.book_imbalance_ratio_threshold
        )
        short_eligible = (
            short_added >= self.whale_threshold
            and imbalance <= (1 - self.book_imbalance_ratio_threshold)
        )
        if long_eligible and short_eligible:
            return "long" if long_added >= short_added else "short"
        if long_eligible:
            return "long"
        if short_eligible:
            return "short"
        return None

    def _is_market_side_on_cooldown(self, market_id: str, side: str, timestamp: datetime) -> bool:
        cooldown_key = (market_id, side)
        cooldown_until = self._market_cooldowns.get(cooldown_key)
        if cooldown_until is None:
            return False
        if timestamp >= cooldown_until:
            del self._market_cooldowns[cooldown_key]
            return False
        return True

    def _set_market_cooldown(self, market_id: str, timestamp: datetime) -> None:
        cooldown_until = timestamp + timedelta(seconds=MARKET_SIGNAL_COOLDOWN_SECONDS)
        for side in ("long", "short"):
            self._market_cooldowns[(market_id, side)] = cooldown_until

    def _record_mid_price(self, snapshot: PerpSnapshot) -> None:
        if snapshot.mid_price is None:
            return
        self._mid_price_history[snapshot.asset_id].append((snapshot.timestamp, snapshot.mid_price))

    def _record_open_interest(self, snapshot: PerpSnapshot) -> None:
        if snapshot.open_interest <= 0:
            return
        history = self._open_interest_history[snapshot.asset_id]
        if history and history[-1][1] == snapshot.open_interest:
            return
        history.append((snapshot.timestamp, snapshot.open_interest))

    def _trim_histories(self, snapshot: PerpSnapshot) -> None:
        aggression_cutoff = snapshot.timestamp - timedelta(seconds=self.config.aggression_window_seconds)
        aggression_history = self._aggression_history[snapshot.asset_id]
        while aggression_history and aggression_history[0].timestamp < aggression_cutoff:
            aggression_history.popleft()

        price_cutoff = snapshot.timestamp - timedelta(seconds=self.config.price_impact_window_seconds)
        price_history = self._mid_price_history[snapshot.asset_id]
        while price_history and price_history[0][0] < price_cutoff:
            price_history.popleft()

        oi_cutoff = snapshot.timestamp - timedelta(seconds=OI_SPIKE_WINDOW_SECONDS)
        oi_history = self._open_interest_history[snapshot.asset_id]
        while oi_history and oi_history[0][0] < oi_cutoff:
            oi_history.popleft()

    @staticmethod
    def _size_at_price(levels: tuple, price: float) -> float:
        for level in levels:
            if level.price == price:
                return level.size
        return 0.0

    def _top_of_book_added_notional(self, previous: PerpSnapshot, current: PerpSnapshot) -> tuple[float, float]:
        long_added = 0.0
        short_added = 0.0
        if current.bids:
            current_bid = current.bids[0]
            previous_size = self._size_at_price(previous.bids, current_bid.price)
            long_added = current_bid.price * max(current_bid.size - previous_size, 0.0)
        if current.asks:
            current_ask = current.asks[0]
            previous_size = self._size_at_price(previous.asks, current_ask.price)
            short_added = current_ask.price * max(current_ask.size - previous_size, 0.0)
        return long_added, short_added

    def _aggression_total(self, asset_id: int, side: str, window_seconds: int) -> float:
        history = self._aggression_history[asset_id]
        if not history:
            return 0.0
        cutoff = history[-1].timestamp - timedelta(seconds=window_seconds)
        return sum(
            event.added_notional
            for event in history
            if event.side == side and event.timestamp >= cutoff
        )

    def _book_imbalance_ratio(self, snapshot: PerpSnapshot) -> float:
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
        return (current_mid - baseline_mid) / baseline_mid

    def _open_interest_change(self, asset_id: int) -> float:
        history = self._open_interest_history[asset_id]
        if len(history) < 2:
            return 0.0
        baseline_oi = history[0][1]
        current_oi = history[-1][1]
        if baseline_oi == 0:
            return 0.0
        return (current_oi - baseline_oi) / baseline_oi

    @staticmethod
    def _direction_from_context(price_impact: float, imbalance: float) -> str:
        if price_impact > 0:
            return "long"
        if price_impact < 0:
            return "short"
        return "long" if imbalance >= 0.5 else "short"

    @staticmethod
    def _resolve_side(trigger_sides: list[str]) -> str:
        counts = Counter(trigger_sides)
        if counts["long"] == counts["short"]:
            return trigger_sides[-1]
        return counts.most_common(1)[0][0]

    def _confidence(
        self,
        snapshot: PerpSnapshot,
        triggers: list[str],
        side: str,
        price_impact: float,
        trigger_oi_spike: bool,
    ) -> tuple[float, bool]:
        base_scores = {
            "single_step_liquidity": 0.6,
            "rapid_sequential_aggression": 0.7,
            "price_impact": 0.75,
            "oi_spike": 0.65,
            "funding_divergence": 0.65,
        }
        confidence = max(base_scores[trigger] for trigger in triggers)
        wallet_bonus_applied = False

        if len(triggers) >= 2:
            confidence += 0.1
        if trigger_oi_spike and ((side == "long" and price_impact > 0) or (side == "short" and price_impact < 0)):
            confidence += 0.1
        imbalance = self._book_imbalance_ratio(snapshot)
        if (side == "long" and imbalance >= self.book_imbalance_ratio_threshold) or (
            side == "short" and imbalance <= (1 - self.book_imbalance_ratio_threshold)
        ):
            confidence += 0.1
        if snapshot.trigger_address:
            wallet_score = self.storage.get_wallet_score(snapshot.trigger_address)
            if wallet_score is not None and wallet_score.win_rate >= 0.65 and wallet_score.trade_count >= 8:
                confidence += 0.15
                wallet_bonus_applied = True
        return min(confidence, 1.0), wallet_bonus_applied