from __future__ import annotations

import logging
from uuid import uuid4

from config import Config
from .adapter import HyperliquidAdapter
from .models import OrderIntent, OrderResult, PerpMarket
from .storage import Storage

LOGGER = logging.getLogger(__name__)


class OrderExecutor:
    def __init__(self, config: Config, storage: Storage, adapter: HyperliquidAdapter) -> None:
        self.config = config
        self.storage = storage
        self.adapter = adapter

    async def place_limit_order(
        self,
        market_id: str,
        asset_id: int,
        side: str,
        size_usdh: float,
        price: float,
        signal_id: int | None = None,
    ) -> OrderResult:
        self._validate(price, size_usdh)
        quantity = self._contracts_for_notional(size_usdh, price)
        intent = OrderIntent(
            market_id=market_id,
            asset_id=asset_id,
            side=side,
            size_usdh=size_usdh,
            quantity=quantity,
            price=price,
            client_order_id=str(uuid4()),
            paper_trade=self.config.paper_trade,
            signal_id=signal_id,
        )
        self.storage.insert_order_intent(
            intent,
            status="pending",
            details={"mode": "paper" if self.config.paper_trade else "live", "quantity": quantity},
        )

        if self.config.paper_trade:
            result = OrderResult(
                order_id=intent.client_order_id,
                status="paper_filled",
                filled_price=price,
                client_order_id=intent.client_order_id,
                details={"mode": "paper", "quantity": quantity},
            )
        else:
            result = await self.adapter.submit_limit_order(intent)

        self.storage.update_order_result(intent.client_order_id, result)
        return result

    async def place_perp_limit_order(
        self,
        market: PerpMarket,
        side: str,
        size_usdh: float,
        limit_price: float,
        source_mid_price: float | None = None,
        signal_id: int | None = None,
    ) -> OrderResult:
        self._validate_perp(limit_price, size_usdh)
        LOGGER.info(
            "Perp order placement inputs market=%s coin=%s side=%s source_mid_price=%s limit_price=%.4f size_usdh=%.2f",
            market.market_id,
            market.coin,
            side,
            f"{source_mid_price:.4f}" if source_mid_price is not None else "n/a",
            limit_price,
            size_usdh,
        )
        quantity = self._perp_quantity_for_notional(size_usdh, limit_price)
        intent = OrderIntent(
            market_id=market.market_id,
            asset_id=market.asset_id,
            side=side,
            size_usdh=size_usdh,
            quantity=quantity,
            price=limit_price,
            client_order_id=str(uuid4()),
            paper_trade=self.config.paper_trade,
            signal_id=signal_id,
        )
        self.storage.insert_perp_order_intent(
            intent,
            status="pending",
            details={"mode": "paper" if self.config.paper_trade else "live", "quantity": quantity, "coin": market.coin},
        )

        if self.config.paper_trade:
            result = OrderResult(
                order_id=intent.client_order_id,
                status="paper_filled",
                filled_price=limit_price,
                client_order_id=intent.client_order_id,
                details={"mode": "paper", "quantity": quantity, "coin": market.coin},
            )
        else:
            result = await self.adapter.submit_limit_order(intent)

        self.storage.update_perp_order_result(intent.client_order_id, result)
        return result

    @staticmethod
    def _validate(price: float, size_usdh: float) -> None:
        if not 0.001 <= price <= 0.999:
            raise ValueError(f"Price must be within [0.001, 0.999], got {price}")
        if size_usdh < 10:
            raise ValueError(f"Notional size must be at least 10 USDH, got {size_usdh}")

    @staticmethod
    def _contracts_for_notional(size_usdh: float, price: float) -> int:
        contracts = round(size_usdh / price)
        return max(int(contracts), 1)

    @staticmethod
    def _validate_perp(price: float, size_usdh: float) -> None:
        if price <= 0:
            raise ValueError(f"Perp limit price must be positive, got {price}")
        if size_usdh < 10:
            raise ValueError(f"Perp notional size must be at least 10 USDH, got {size_usdh}")

    @staticmethod
    def _perp_quantity_for_notional(size_usdh: float, price: float) -> float:
        quantity = round(size_usdh / price, 6)
        return max(quantity, 0.000001)
