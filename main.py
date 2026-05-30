from __future__ import annotations

import asyncio
import logging
from contextlib import suppress

from config import Config
from modules import HyperliquidAdapter, L2BookFeeder, OrderExecutor, RiskManager, WhaleDetector, discover_btc_daily_market
from modules.models import WhaleSignal
from modules.storage import Storage

LOGGER = logging.getLogger(__name__)


async def signal_handler(
    config: Config,
    signal_queue: asyncio.Queue[WhaleSignal],
    risk_manager: RiskManager,
    executor: OrderExecutor,
) -> None:
    while True:
        signal = await signal_queue.get()
        try:
            if not risk_manager.can_trade(signal.market_id, config.follow_size_usdh):
                LOGGER.info("Risk manager rejected signal for market %s", signal.market_id)
                continue

            mid_price = signal.details.get("mid_price")
            if not isinstance(mid_price, (int, float)):
                LOGGER.info("Skipping signal without mid price for market %s", signal.market_id)
                continue

            price = min(float(mid_price) + 0.01, 0.999) if signal.side == "YES" else max(float(mid_price) - 0.01, 0.001)
            result = await executor.place_limit_order(
                market_id=signal.market_id,
                asset_id=signal.asset_id,
                side="buy",
                size_usdh=config.follow_size_usdh,
                price=price,
                signal_id=signal.signal_id,
            )
            LOGGER.info("Placed %s order for %s at %.4f via %s", signal.side, signal.market_id, price, result.status)
        finally:
            signal_queue.task_done()


async def discovery_loop(
    config: Config,
    adapter: HyperliquidAdapter,
    storage: Storage,
    snapshot_queue: asyncio.Queue,
) -> None:
    active_market_id: str | None = None
    feeder: L2BookFeeder | None = None
    feeder_task: asyncio.Task | None = None

    try:
        while True:
            market = await discover_btc_daily_market(adapter)
            if market is None:
                LOGGER.info("No BTC daily market discovered on %s", config.rest_url)
            elif market.market_id != active_market_id:
                LOGGER.info("Switching active market to %s", market.market_id)
                if feeder is not None:
                    await feeder.stop()
                if feeder_task is not None:
                    feeder_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await feeder_task
                feeder = L2BookFeeder(config, storage, snapshot_queue, market)
                feeder_task = asyncio.create_task(feeder.run(), name=f"feeder-{market.market_id}")
                active_market_id = market.market_id

            await asyncio.sleep(config.discovery_interval_seconds)
    finally:
        if feeder is not None:
            await feeder.stop()
        if feeder_task is not None:
            feeder_task.cancel()
            with suppress(asyncio.CancelledError):
                await feeder_task


async def async_main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = Config.from_env()
    storage = Storage(config.db_path)
    storage.initialize()
    adapter = HyperliquidAdapter(config)
    risk_manager = RiskManager(config, storage)
    executor = OrderExecutor(config, storage, adapter)
    snapshot_queue: asyncio.Queue = asyncio.Queue()
    signal_queue: asyncio.Queue = asyncio.Queue()
    detector = WhaleDetector(config, storage, snapshot_queue, signal_queue)
    LOGGER.info(
        "Remora initialized for %s in %s mode",
        "testnet" if config.testnet else "mainnet",
        "paper" if config.paper_trade else "live",
    )

    detector_task = asyncio.create_task(detector.run(), name="detector")
    signal_task = asyncio.create_task(signal_handler(config, signal_queue, risk_manager, executor), name="signal-handler")
    discovery_task = asyncio.create_task(discovery_loop(config, adapter, storage, snapshot_queue), name="discovery-loop")

    try:
        await asyncio.gather(detector_task, signal_task, discovery_task)
    finally:
        for task in (discovery_task, signal_task, detector_task):
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        await adapter.close()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
