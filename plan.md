# HyperWhal — Hyperliquid HIP-4 Whale Bot
> Prediction market whale-following bot on Hyperliquid HIP-4
> No KYC · No geo-block · Legal from the Netherlands · Zero open fees

---

## What This Bot Does

Monitors the Hyperliquid HIP-4 prediction market order book in real time.
When a "whale" (large smart-money trader) places a big order, the bot detects it
and mirrors the position at a slightly better price — betting the whale knows something.

Everything runs on-chain. No account registration, no country restrictions.
You authenticate with a private key from an ETH wallet.

---

## Markets Available (as of May 2026)

| Market | Type | Cadence | Best for |
|---|---|---|---|
| BTC daily binary | Binary YES/NO | Daily reset | Bot development, most liquid |
| BTC 15-min price buckets | Multi-bucket | Every 15 min | High frequency signals |
| US CPI print (June 10) | Macro event | One-shot | High alpha, thin liquidity |
| Fed rate decision | Offchain event | Per meeting | Whale signals from macro traders |

> **Strategy note:** CPI and Fed markets have the most alpha — macro traders with
> Bloomberg terminals and ex-Fed contacts trade these. Early large orders before a
> print are a very strong signal. Start with BTC daily for development, then add macro.

---

## Tech Stack

```
Python 3.11+
├── hyperliquid-python-sdk   — signing + REST calls
├── eth_account              — wallet management
├── websockets               — real-time order book feed
├── pandas / numpy           — signal logic
├── python-dotenv            — env config
├── aiohttp                  — async HTTP
└── SQLite (built-in)        — trade log + backtest data
```

**Reference repo (read this first):**
`git clone https://github.com/chainstacklabs/hyperliquid-hip-4`
12 progressive Python examples covering every HIP-4 primitive.

---

## Project Structure

```
hyperwhal/
├── .env                      ← private key + config (never commit)
├── .gitignore
├── requirements.txt
├── main.py                   ← async entrypoint
├── config.py                 ← constants + env loading
├── modules/
│   ├── __init__.py
│   ├── market_discovery.py   ← poll outcomeMeta, find active markets
│   ├── feeder.py             ← WebSocket order book stream
│   ├── detector.py           ← whale signal logic
│   ├── executor.py           ← place/cancel orders
│   └── risk_manager.py       ← position limits + circuit breaker
└── data/
    └── trades.db             ← SQLite log
```

---

## .env File

```env
HL_PRIVATE_KEY=your_wallet_private_key_here
HL_WALLET_ADDRESS=your_wallet_address_here
HL_TESTNET=true
```

---

## Constants (config.py)

| Constant | Value | Meaning |
|---|---|---|
| `WHALE_SIZE_THRESHOLD` | 500 USDH | Min order size to trigger signal |
| `PRICE_IMPACT_THRESHOLD` | 0.03 | 3% mid price move in <30s |
| `MAX_POSITION_USDH` | 200 | Max total exposure |
| `DRAWDOWN_LIMIT` | 0.15 | 15% drawdown → circuit breaker halts trading |
| `FOLLOW_SIZE_USDH` | 50 | Size of each follow trade |
| `POLL_INTERVAL_SECONDS` | 5 | Market discovery refresh |

---

## Module Prompts

Use these one at a time in a **new Claude chat** for each file.

---

### `config.py`

```
Write a Python config.py for a Hyperliquid HIP-4 trading bot.
Load env vars using python-dotenv: HL_PRIVATE_KEY, HL_WALLET_ADDRESS,
HL_TESTNET (bool). Export a single Config dataclass with those fields
plus constants: WHALE_SIZE_THRESHOLD = 500 (USDH), PRICE_IMPACT_THRESHOLD = 0.03,
MAX_POSITION_USDH = 200, DRAWDOWN_LIMIT = 0.15, FOLLOW_SIZE_USDH = 50,
POLL_INTERVAL_SECONDS = 5.
```

---

### `modules/market_discovery.py`

```
Write a Python async module for a Hyperliquid HIP-4 bot.
Use the hyperliquid-python-sdk to call the /info endpoint with type outcomeMeta.
Parse all active HIP-4 markets and return a list of dataclasses with fields:
market_id, title, underlying, expiry, yes_asset_id, no_asset_id,
market_type (binary or bucket).
Support both testnet and mainnet via a base_url param.
```

---

### `modules/feeder.py`

```
Write a Python async WebSocket feeder for Hyperliquid HIP-4.
Connect to the Hyperliquid WebSocket (wss://api.hyperliquid.xyz/ws or testnet equivalent).
Subscribe to the l2Book feed for a list of asset IDs (YES and NO sides of HIP-4 markets).
On each message, parse the order book snapshot and push it to an asyncio Queue.
Handle reconnection with exponential backoff.
```

---

### `modules/detector.py`

```
Write a Python whale detector module for a Hyperliquid HIP-4 bot.
It reads from an asyncio Queue of order book snapshots.
Detect whale signals using three criteria:
  (1) a single order with size >= WHALE_SIZE_THRESHOLD USDH
  (2) rapid sequential orders on the same side within 10 seconds
      totalling >= WHALE_SIZE_THRESHOLD
  (3) mid-price impact >= PRICE_IMPACT_THRESHOLD in under 30 seconds
When a signal fires, emit a WhaleSignal dataclass with fields:
  market_id, side (YES/NO), confidence (0-1), timestamp, trigger_type.
Log everything to SQLite.
```

---

### `modules/executor.py`

```
Write a Python async executor for a Hyperliquid HIP-4 bot.
Use the hyperliquid-python-sdk to place limit orders on HIP-4 outcome markets.
Inputs: wallet private key, asset_id, side (buy/sell), size_usdh, price.
Use client_order_id (UUID4) to prevent duplicates.
Support a paper_trade=True flag that logs the order to SQLite
without actually submitting.
Return an OrderResult dataclass with order_id, status, filled_price.
```

---

### `modules/risk_manager.py`

```
Write a Python risk manager for a Hyperliquid HIP-4 bot.
Track open positions and total exposure in USDH from SQLite.
Expose a method can_trade(market_id, size_usdh) -> bool that returns False if:
  - total exposure >= MAX_POSITION_USDH, or
  - drawdown from peak balance >= DRAWDOWN_LIMIT (circuit breaker), or
  - we already have a position in this market_id.
Expose update_balance(new_balance_usdh) to track PnL.
```

---

### `main.py`

```
Write the main async entrypoint for a Hyperliquid HIP-4 whale-following trading bot.
It should:
  (1) load config
  (2) run market_discovery every 60 seconds to get active markets
  (3) start the feeder WebSocket for all discovered asset IDs
  (4) run the detector on the feeder queue
  (5) when a WhaleSignal fires and risk_manager.can_trade() returns True,
      call executor to place a limit order on the same side as the whale —
      mid+0.01 for YES or mid-0.01 for NO, size = FOLLOW_SIZE_USDH
Run everything as asyncio tasks with graceful shutdown on CTRL+C.
```

---

## Build Order (45 days before Eindhoven)

| Week | Goal |
|---|---|
| **Week 1** | Clone reference repo. Generate ETH wallet. Set up project structure. Get `market_discovery.py` working against testnet — log active markets to console. Swap USDC → USDH on testnet using the faucet script in the reference repo. |
| **Week 2** | Build `feeder.py` — stream live order book. Build `detector.py` — log whale signals to SQLite. Backtest signal quality on stored data. |
| **Week 3** | Build `executor.py` with `paper_trade=True`. Build `risk_manager.py`. Wire everything in `main.py`. Run full loop on testnet with no real money. |
| **Week 4** | Flip `paper_trade=False` and `HL_TESTNET=false`. Go live with ~$50 USDH. Monitor, tune thresholds, iterate. |
| **Eindhoven 🇳🇱** | Deploy to a VPS (Hetzner ~€4/mo). Scale up. Fully legal, no geo-block, no KYC. |

---

## Wallet Setup (do this first)

1. Generate a new ETH wallet — use MetaMask or `eth_account` in Python
2. Fund with USDC on Arbitrum (Hyperliquid bridges from Arbitrum)
3. Deposit to Hyperliquid via https://app.hyperliquid.xyz
4. Swap USDC → USDH on the @1338 / @230 spot pair (HIP-4 settles in USDH)
5. For testnet: use the faucet at https://app.hyperliquid-testnet.xyz

> **Security:** Never put your main wallet private key in .env.
> Generate a separate "hot wallet" just for this bot with limited funds.

---

## Key Gotchas

- HIP-4 asset IDs follow `20000 + outcome_index` — different from perp IDs
- YES mid + NO mid ≈ 1.0 always (they're complementary probabilities)
- USDH not USDC — make sure you swap first or orders will fail
- The SDK may not have full HIP-4 support yet (28 days old) —
  fall back to raw HTTP calls from `chainstacklabs/hyperliquid-hip-4` if needed
- Markets on testnet reset frequently — don't be alarmed if outcomeMeta is empty

---

## If the SDK is missing HIP-4 support

Add this prompt to the relevant module chat:

```
The hyperliquid-python-sdk doesn't support HIP-4 outcomeMeta yet.
Use raw aiohttp POST requests to https://api.hyperliquid-testnet.xyz/info
with body {"type": "outcomeMeta"} to fetch markets.
For order placement, use the signing approach from
github.com/chainstacklabs/hyperliquid-hip-4 —
sign with eth_account and POST to /exchange.
```

---

## Resources

| Link | What |
|---|---|
| https://github.com/chainstacklabs/hyperliquid-hip-4 | Reference bot repo (12 examples) |
| https://docs.chainstack.com/docs/hyperliquid-hip4-outcome-markets-trading | Full API docs |
| https://app.hyperliquid-testnet.xyz | Testnet UI |
| https://app.hyperliquid.xyz | Mainnet UI |
| https://www.artemis.ai/company/HYPE?tab=hip4 | HIP-4 volume analytics |

---

