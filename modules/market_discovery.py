from __future__ import annotations

from dataclasses import dataclass

from .adapter import HyperliquidAdapter, encode_asset_id, encode_coin
from .models import MarketInfo


@dataclass(frozen=True)
class OutcomeDescription:
    outcome_class: str
    underlying: str
    expiry: str | None
    target_price: float | None
    period: str | None


def parse_description(description: str) -> OutcomeDescription:
    parts = dict(part.split(":", 1) for part in description.split("|") if ":" in part)
    target_price = parts.get("targetPrice")
    return OutcomeDescription(
        outcome_class=parts.get("class", "unknown"),
        underlying=parts.get("underlying", "unknown"),
        expiry=parts.get("expiry"),
        target_price=float(target_price) if target_price is not None else None,
        period=parts.get("period"),
    )


def _market_type(outcome_class: str) -> str:
    if outcome_class == "priceBinary":
        return "binary"
    return "bucket"


def _build_market_info(outcome: dict) -> MarketInfo:
    outcome_id = int(outcome["outcome"])
    parsed = parse_description(str(outcome.get("description", "")))
    title = f"{parsed.underlying} {parsed.period or ''} {parsed.expiry or ''}".strip()
    return MarketInfo(
        market_id=str(outcome_id),
        title=title,
        underlying=parsed.underlying,
        expiry=parsed.expiry,
        yes_asset_id=encode_asset_id(outcome_id, 0),
        no_asset_id=encode_asset_id(outcome_id, 1),
        yes_coin=encode_coin(outcome_id, 0),
        no_coin=encode_coin(outcome_id, 1),
        market_type=_market_type(parsed.outcome_class),
        universe_index=outcome_id,
    )


async def discover_active_markets(adapter: HyperliquidAdapter) -> list[MarketInfo]:
    outcome_meta = await adapter.fetch_outcome_meta()
    adapter.patch_sdk_outcome_assets(outcome_meta)
    return [_build_market_info(outcome) for outcome in outcome_meta.get("outcomes", [])]


async def discover_btc_daily_market(adapter: HyperliquidAdapter) -> MarketInfo | None:
    outcome_meta = await adapter.fetch_outcome_meta()
    adapter.patch_sdk_outcome_assets(outcome_meta)
    for outcome in outcome_meta.get("outcomes", []):
        parsed = parse_description(str(outcome.get("description", "")))
        if parsed.underlying == "BTC" and parsed.outcome_class == "priceBinary" and parsed.period == "1d":
            return _build_market_info(outcome)
    return None
