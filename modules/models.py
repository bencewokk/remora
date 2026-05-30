from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


@dataclass(frozen=True)
class DepthLevel:
    price: float
    size: float


@dataclass(frozen=True)
class MarketInfo:
    market_id: str
    title: str
    underlying: str
    expiry: str | None
    yes_asset_id: int
    no_asset_id: int
    yes_coin: str
    no_coin: str
    market_type: str
    universe_index: int | None = None


@dataclass(frozen=True)
class PerpMarket:
    market_id: str
    coin: str
    asset_id: int
    mark_price: float | None
    funding_rate: float
    open_interest: float


@dataclass(frozen=True)
class OrderBookSnapshot:
    market_id: str
    asset_id: int
    outcome_side: str
    bids: tuple[DepthLevel, ...]
    asks: tuple[DepthLevel, ...]
    best_bid: float | None
    best_ask: float | None
    mid_price: float | None
    timestamp: datetime = field(default_factory=utc_now)
    sequence_id: int | None = None
    is_reset: bool = False
    trigger_address: str | None = None
    raw_message: dict[str, Any] | None = None


@dataclass(frozen=True)
class PerpSnapshot:
    market_id: str
    coin: str
    asset_id: int
    timestamp: datetime = field(default_factory=utc_now)
    sequence_id: int | None = None
    bids: tuple[DepthLevel, ...] = field(default_factory=tuple)
    asks: tuple[DepthLevel, ...] = field(default_factory=tuple)
    best_bid: float | None = None
    best_ask: float | None = None
    mid_price: float | None = None
    funding_rate: float = 0.0
    open_interest: float = 0.0
    oi_change_pct: float = 0.0
    is_reset: bool = False
    trigger_address: str | None = None
    raw_message: dict[str, Any] | None = None


@dataclass(frozen=True)
class WhaleSignal:
    market_id: str
    asset_id: int
    side: str
    confidence: float
    trigger_type: str
    timestamp: datetime = field(default_factory=utc_now)
    signal_id: int | None = None
    wallet_bonus_applied: bool = False
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PerpWhaleSignal:
    market_id: str
    coin: str
    asset_id: int
    side: str
    confidence: float
    trigger_type: str
    timestamp: datetime = field(default_factory=utc_now)
    signal_id: int | None = None
    wallet_bonus_applied: bool = False
    trigger_oi_spike: bool = False
    trigger_funding: bool = False
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class WalletScore:
    address: str
    trade_count: int
    win_count: int
    win_rate: float
    total_pnl_usdh: float
    avg_pnl_per_trade: float
    last_trade_ts: int | None
    last_updated_ts: int


@dataclass(frozen=True)
class OrderIntent:
    market_id: str
    asset_id: int
    side: str
    size_usdh: float
    quantity: float
    price: float
    client_order_id: str
    paper_trade: bool
    timestamp: datetime = field(default_factory=utc_now)
    signal_id: int | None = None


@dataclass(frozen=True)
class OrderResult:
    order_id: str
    status: str
    filled_price: float | None
    client_order_id: str
    timestamp: datetime = field(default_factory=utc_now)
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PositionState:
    market_id: str
    side: str
    exposure_usdh: float
    average_price: float
    quantity: float
    opened_at: datetime
    resolved_at: datetime | None = None
