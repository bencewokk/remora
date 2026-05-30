from __future__ import annotations

from typing import Any

import aiohttp

from config import Config
from .models import OrderIntent, OrderResult

try:
    from hyperliquid.info import Info  # type: ignore
except ImportError:  # pragma: no cover - dependency may not be installed yet
    Info = None


OUTCOME_ASSET_BASE = 100_000_000


def encode_coin(outcome_id: int, side: int) -> str:
    if side not in (0, 1):
        raise ValueError(f"side must be 0 or 1, got {side}")
    return f"#{10 * outcome_id + side}"


def encode_balance_coin(outcome_id: int, side: int) -> str:
    if side not in (0, 1):
        raise ValueError(f"side must be 0 or 1, got {side}")
    return f"+{10 * outcome_id + side}"


def encode_asset_id(outcome_id: int, side: int) -> int:
    if side not in (0, 1):
        raise ValueError(f"side must be 0 or 1, got {side}")
    return OUTCOME_ASSET_BASE + 10 * outcome_id + side


class HyperliquidAdapter:
    def __init__(self, config: Config) -> None:
        self.config = config
        self._session: aiohttp.ClientSession | None = None
        self._sdk_info = self._build_sdk_info()

    def _build_sdk_info(self) -> Any | None:
        if Info is None:
            return None
        skip_ws = True
        try:
            return Info(self.config.rest_url, skip_ws=skip_ws)
        except TypeError:
            try:
                return Info(base_url=self.config.rest_url, skip_ws=skip_ws)
            except TypeError:
                return None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def fetch_outcome_meta(self) -> dict[str, Any]:
        if self._sdk_info is not None and hasattr(self._sdk_info, "post"):
            try:
                return self._sdk_info.post("/info", {"type": "outcomeMeta"})
            except Exception:
                pass

        session = await self._get_session()
        async with session.post(f"{self.config.rest_url}/info", json={"type": "outcomeMeta"}) as response:
            response.raise_for_status()
            return await response.json()

    async def fetch_l2_book(self, coin: str) -> dict[str, Any]:
        if self._sdk_info is not None and hasattr(self._sdk_info, "l2_snapshot"):
            try:
                return self._sdk_info.l2_snapshot(coin)
            except Exception:
                pass

        session = await self._get_session()
        async with session.post(f"{self.config.rest_url}/info", json={"type": "l2Book", "coin": coin}) as response:
            response.raise_for_status()
            return await response.json()

    def patch_sdk_outcome_assets(self, outcome_meta: dict[str, Any]) -> None:
        if self._sdk_info is None:
            return
        coin_to_asset = getattr(self._sdk_info, "coin_to_asset", None)
        name_to_coin = getattr(self._sdk_info, "name_to_coin", None)
        if coin_to_asset is None or name_to_coin is None:
            return
        for outcome in outcome_meta.get("outcomes", []):
            outcome_id = int(outcome["outcome"])
            for side in (0, 1):
                coin = encode_coin(outcome_id, side)
                if coin in coin_to_asset:
                    continue
                coin_to_asset[coin] = encode_asset_id(outcome_id, side)
                name_to_coin[coin] = coin

    async def submit_limit_order(self, order: OrderIntent) -> OrderResult:
        raise NotImplementedError("Live order submission will be implemented in executor-backed adapter flow.")

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()