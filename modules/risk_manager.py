from __future__ import annotations

from dataclasses import dataclass

from config import Config
from .storage import Storage


@dataclass(frozen=True)
class RiskSnapshot:
    total_reserved_exposure: float
    peak_equity: float
    current_equity: float
    drawdown: float
    open_markets: set[str]


class RiskManager:
    def __init__(self, config: Config, storage: Storage) -> None:
        self.config = config
        self.storage = storage
        if config.starting_cash_usdh > 0:
            ledger = self.storage.get_ledger_state()
            if float(ledger["cash_balance"]) == 0 and float(ledger["equity"]) == 0:
                self.storage.update_ledger_state(
                    cash_balance=config.starting_cash_usdh,
                    equity=config.starting_cash_usdh,
                    peak_equity=config.starting_cash_usdh,
                )

    def can_trade(self, market_id: str, size_usdh: float) -> bool:
        snapshot = self.snapshot()
        if market_id in snapshot.open_markets:
            return False
        if snapshot.total_reserved_exposure + size_usdh > self.config.max_position_usdh:
            return False
        if snapshot.drawdown >= self.config.drawdown_limit:
            return False
        return True

    def snapshot(self) -> RiskSnapshot:
        ledger = self.storage.get_ledger_state()
        current_equity = float(ledger["equity"])
        peak_equity = float(ledger["peak_equity"])
        drawdown = 0.0
        if peak_equity > 0:
            drawdown = max((peak_equity - current_equity) / peak_equity, 0.0)
        return RiskSnapshot(
            total_reserved_exposure=self.storage.total_reserved_exposure(),
            peak_equity=peak_equity,
            current_equity=current_equity,
            drawdown=drawdown,
            open_markets=self.storage.open_market_ids(),
        )

    def update_balance(self, new_balance_usdh: float) -> None:
        ledger = self.storage.get_ledger_state()
        peak_equity = max(float(ledger["peak_equity"]), new_balance_usdh)
        self.storage.update_ledger_state(
            cash_balance=new_balance_usdh,
            equity=new_balance_usdh,
            peak_equity=peak_equity,
        )
