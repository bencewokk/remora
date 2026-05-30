from __future__ import annotations

from uuid import uuid4

from config import Config
from .adapter import HyperliquidAdapter
from .models import OrderIntent, OrderResult
from .storage import Storage


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
