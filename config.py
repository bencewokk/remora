from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def _parse_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


@dataclass(frozen=True)
class Config:
    private_key: str | None
    wallet_address: str | None
    testnet: bool
    paper_trade: bool
    whale_size_threshold: float
    price_impact_threshold: float
    max_position_usdh: float
    drawdown_limit: float
    follow_size_usdh: float
    starting_cash_usdh: float
    discovery_interval_seconds: int
    wallet_refresh_seconds: int
    poll_interval_seconds: int
    aggression_window_seconds: int
    sequential_window_seconds: int
    price_impact_window_seconds: int
    book_imbalance_levels: int
    book_imbalance_ratio_threshold: float
    db_path: Path
    rest_url: str
    ws_url: str

    @classmethod
    def from_env(cls) -> "Config":
        load_dotenv()
        root = Path(__file__).resolve().parent
        testnet = _parse_bool(os.getenv("HL_TESTNET"), True)
        return cls(
            private_key=os.getenv("HL_PRIVATE_KEY"),
            wallet_address=os.getenv("HL_WALLET_ADDRESS"),
            testnet=testnet,
            paper_trade=_parse_bool(os.getenv("HL_PAPER_TRADE"), False),
            whale_size_threshold=float(os.getenv("WHALE_SIZE_THRESHOLD", "500")),
            price_impact_threshold=float(os.getenv("PRICE_IMPACT_THRESHOLD", "0.03")),
            max_position_usdh=float(os.getenv("MAX_POSITION_USDH", "200")),
            drawdown_limit=float(os.getenv("DRAWDOWN_LIMIT", "0.15")),
            follow_size_usdh=float(os.getenv("FOLLOW_SIZE_USDH", "50")),
            starting_cash_usdh=float(os.getenv("STARTING_CASH_USDH", "0")),
            discovery_interval_seconds=int(os.getenv("DISCOVERY_INTERVAL_SECONDS", "60")),
            wallet_refresh_seconds=int(os.getenv("WALLET_REFRESH_SECONDS", "300")),
            poll_interval_seconds=int(os.getenv("POLL_INTERVAL_SECONDS", "5")),
            aggression_window_seconds=int(os.getenv("AGGRESSION_WINDOW_SECONDS", "60")),
            sequential_window_seconds=int(os.getenv("SEQUENTIAL_WINDOW_SECONDS", "10")),
            price_impact_window_seconds=int(os.getenv("PRICE_IMPACT_WINDOW_SECONDS", "30")),
            book_imbalance_levels=int(os.getenv("BOOK_IMBALANCE_LEVELS", "3")),
            book_imbalance_ratio_threshold=float(os.getenv("BOOK_IMBALANCE_RATIO_THRESHOLD", "0.7")),
            db_path=Path(os.getenv("TRADES_DB_PATH", str(root / "data" / "trades.db"))),
            rest_url=(
                os.getenv("HL_REST_URL", "https://api.hyperliquid-testnet.xyz")
                if testnet
                else os.getenv("HL_REST_URL", "https://api.hyperliquid.xyz")
            ),
            ws_url=(
                os.getenv("HL_WS_URL", "wss://api.hyperliquid-testnet.xyz/ws")
                if testnet
                else os.getenv("HL_WS_URL", "wss://api.hyperliquid.xyz/ws")
            ),
        )
