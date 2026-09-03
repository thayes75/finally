# Massive API Reference (formerly Polygon.io)

Reference documentation for the Massive stock market data API as used in FinAlly. Researched from the live Massive documentation (massive.com/docs) and the official Python client's README/source in September 2026.

## Background

Polygon.io rebranded to **Massive** on **October 30, 2025**. It's the same platform, company, and data under a new name:

- Base URL moved from `api.polygon.io` to `api.massive.com` (the old domain still resolves, for backward compatibility, but new code should target the new one)
- The PyPI package was renamed from `polygon-api-client` to `massive`
- Existing API keys continue to work unchanged
- All REST paths, request/response shapes, and SDK method signatures are unchanged from Polygon.io — this doc's endpoint paths (`/v2/...`, `/v3/...`) are identical to what existing Polygon.io material describes

## Overview

- **Base URL**: `https://api.massive.com`
- **Docs**: https://massive.com/docs
- **Python package**: `massive` — install via `pip install -U massive` / `uv add massive`
- **Min Python version**: 3.9+
- **Repo**: https://github.com/massive-com/client-python
- **Auth**: API key from the [Massive dashboard](https://massive.com/dashboard/api-keys), passed to `RESTClient(api_key=...)` or read from the `MASSIVE_API_KEY` (previously `POLYGON_API_KEY`) environment variable
- **Auth header**: `Authorization: Bearer <API_KEY>` — the client attaches this automatically

## Rate Limits & Data Recency

| Tier | Requests | Data recency |
|------|----------|--------------|
| Free | 5 requests/minute | End-of-day |
| Stocks Starter | Unlimited (soft cap ~100 req/s) | 15-minute delayed |
| Stocks Developer | Unlimited | 15-minute delayed |
| Stocks Advanced | Unlimited | Real-time |
| Stocks Business | Unlimited | Real-time + fair market value (`fmv`) |

Two things worth calling out, since they affect how FinAlly should set expectations:

1. **"Real-time" only applies to Advanced/Business plans.** A `MASSIVE_API_KEY` on the Free or Starter/Developer tiers still returns valid prices, but they lag the actual market by up to 15 minutes (or are end-of-day only on Free). FinAlly should not claim real-time data unless the configured key is on a real-time-eligible plan — there's no reliable way to introspect the plan tier from the API response, so this is a documentation/expectation-setting concern, not something to branch on in code.
2. **Free tier is throttled hard** (5 req/min). Polling faster than that returns HTTP 429. For FinAlly's polling design, this means the poll interval must be tier-aware or conservatively defaulted (see below).

## Client Initialization

```python
from massive import RESTClient

# Reads MASSIVE_API_KEY from environment automatically
client = RESTClient()

# Or pass explicitly
client = RESTClient(api_key="your_key_here")

# Debugging: log request/response URLs and headers (auth redacted)
client = RESTClient(api_key="your_key_here", trace=True, verbose=True)
```

## Endpoints Used in FinAlly

### 1. Full Market Snapshot — Multiple Tickers (Primary Endpoint)

Gets current prices for a set of tickers in a **single API call**. This is the endpoint FinAlly's poller uses.

**REST**: `GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,GOOGL,MSFT`

**Query parameters**:
- `tickers` (string, optional) — case-sensitive comma-separated list, e.g. `AAPL,TSLA,GOOGL`. **Always pass this explicitly** — an empty/omitted value returns a snapshot for the entire US stock market (thousands of tickers), which is wasteful and far more likely to hit rate limits.
- `include_otc` (bool, optional, default `false`) — include OTC securities.

**Python client**:
```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient()

snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"],
)

for snap in snapshots:
    print(f"{snap.ticker}: ${snap.last_trade.price}")
    print(f"  Day change: {snap.todays_change_percent}%")
    print(f"  Day OHLC: O={snap.day.open} H={snap.day.high} L={snap.day.low} C={snap.day.close}")
    print(f"  Volume: {snap.day.volume}")
```

**Response structure** (per ticker, abbreviated):
```json
{
  "ticker": "AAPL",
  "day": {
    "o": 129.61, "h": 130.15, "l": 125.07, "c": 125.07,
    "v": 111237700, "vw": 127.35
  },
  "prevDay": { "o": 128.0, "h": 130.0, "l": 127.5, "c": 129.61, "v": 98000000 },
  "lastTrade": { "p": 125.07, "s": 100, "x": 11, "t": 1675190399000000000 },
  "lastQuote": { "p": 125.06, "P": 125.08, "s": 500, "S": 1000, "t": 1675190399500000000 },
  "todaysChange": -4.54,
  "todaysChangePerc": -3.50,
  "updated": 1675190399500283100
}
```

**Field naming note**: the raw JSON uses short keys (`o`/`h`/`l`/`c`/`v`, `p`/`s`/`x`/`t`), but the Python client's typed models expose them as attributes with descriptive names (`snap.day.open`, `snap.last_trade.price`, etc.) — use the attribute names shown in the client examples above, not the raw JSON keys, when working through `RESTClient`.

**Key fields FinAlly extracts**:
- `last_trade.price` — current price for trading and display
- `prev_day.close` — previous session's close, for computing day change if not using `todays_change_percent` directly
- `last_trade.timestamp` — when the price was recorded (nanoseconds — see Timestamps note below)

### 2. Unified Snapshot (v3) — Alternative / Cross-Asset

A newer, more general snapshot endpoint that spans stocks, options, indices, forex, and crypto in one call, addressed by prefixed tickers.

**REST**: `GET /v3/snapshot?ticker.any_of=AAPL,GOOGL,MSFT`

**Query parameters**:
- `ticker.any_of` — up to 250 comma-separated tickers
- `ticker`, `ticker.gte/gt/lte/lt` — single ticker or range filtering
- `type` — restrict to an asset class (`stocks`, `options`, `fx`, `crypto`, `indices`)
- `limit` (default 10, max 250), `sort`, `order` — pagination/ordering

```python
import requests

response = requests.get(
    "https://api.massive.com/v3/snapshot",
    params={"ticker.any_of": "AAPL,GOOGL,MSFT", "limit": 100},
    headers={"Authorization": "Bearer YOUR_API_KEY"},
)
for result in response.json()["results"]:
    print(f"{result['ticker']}: {result.get('last_trade', {}).get('price')}")
```

FinAlly stays with the stocks-only Full Market Snapshot (`/v2/snapshot/.../tickers`, §1) since the watchlist is stocks-only and that endpoint's Python client method (`get_snapshot_all`) is already typed and stable. The v3 unified endpoint is worth knowing about if a future feature needs mixed asset classes (e.g., crypto or index tickers on the watchlist).

### 3. Single Ticker Snapshot

For detailed data on one ticker (e.g., the main chart's selected-ticker header).

```python
snapshot = client.get_snapshot_ticker(
    market_type=SnapshotMarketType.STOCKS,
    ticker="AAPL",
)

print(f"Price: ${snapshot.last_trade.price}")
print(f"Bid/Ask: ${snapshot.last_quote.bid_price} / ${snapshot.last_quote.ask_price}")
print(f"Day range: ${snapshot.day.low} - ${snapshot.day.high}")
```

### 4. Previous Close

Previous trading day's OHLC for a ticker. Useful for computing day-change or as a seed price for a newly-added ticker before the first live snapshot arrives.

**REST**: `GET /v2/aggs/ticker/{ticker}/prev`

```python
prev = client.get_previous_close_agg(ticker="AAPL")

for agg in prev:
    print(f"Previous close: ${agg.close}")
    print(f"OHLC: O={agg.open} H={agg.high} L={agg.low} C={agg.close}")
    print(f"Volume: {agg.volume}")
```

**Response**:
```json
{
  "ticker": "AAPL",
  "results": [
    {"o": 150.0, "h": 155.0, "l": 149.0, "c": 154.5, "v": 1000000, "t": 1672531200000}
  ]
}
```

### 5. Aggregates (Custom Bars)

Historical OHLCV bars over a date range. Not used by the live polling path — relevant only if FinAlly later adds server-rendered historical charts (today the main chart is built client-side from the SSE stream since page load, per `PLAN.md` §6).

**REST**: `GET /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}`

```python
aggs = []
for a in client.list_aggs(
    ticker="AAPL",
    multiplier=1,
    timespan="day",
    from_="2024-01-01",
    to="2024-01-31",
    limit=50000,
):
    aggs.append(a)
```

Response bar fields: `o`, `h`, `l`, `c`, `v` (volume), `vw` (volume-weighted avg price), `t` (Unix ms timestamp), `n` (transaction count). `limit` caps at 50,000 bars per call; the client paginates automatically by default (see Pagination below).

### 6. Last Trade / Last Quote

Individual endpoints for the single most recent trade or NBBO quote — lower-latency than a snapshot but only for one ticker at a time. Not used for polling (the snapshot endpoint above is one call for the whole watchlist), but useful for spot checks.

```python
trade = client.get_last_trade(ticker="AAPL")
print(f"Last trade: ${trade.price} x {trade.size}")

quote = client.get_last_quote(ticker="AAPL")
print(f"Bid: ${quote.bid} x {quote.bid_size}")
print(f"Ask: ${quote.ask} x {quote.ask_size}")
```

## Pagination

Enabled by default on list-style methods (`list_aggs`, `list_trades`, `list_quotes`, etc.) — `limit` controls page size and the client transparently follows `next_url` to fetch every page. Disable it to cap at a single page:

```python
client = RESTClient(api_key="...", pagination=False)
trades = [t for t in client.list_trades(ticker="TSLA", limit=100)]  # max 100 results
```

The snapshot methods FinAlly uses (`get_snapshot_all`, `get_snapshot_ticker`) return a plain list/object, not a paginated iterator — not relevant to the polling path.

## How FinAlly Uses the API

The `MassiveDataSource` poller runs as a background `asyncio` task:

1. Collects all tickers from the watchlist
2. Calls `get_snapshot_all()` with those tickers (one API call, run via `asyncio.to_thread` since the client is synchronous)
3. Extracts `last_trade.price` and `last_trade.timestamp` from each snapshot
4. Writes each into the shared `PriceCache` (see `MARKET_INTERFACE.md`)
5. Sleeps for the poll interval, then repeats

```python
import asyncio
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

async def poll_massive(api_key: str, get_tickers, price_cache, interval: float = 15.0) -> None:
    """Poll Massive and update the price cache. Runs forever until cancelled."""
    client = RESTClient(api_key=api_key)

    while True:
        tickers = get_tickers()
        if tickers:
            snapshots = await asyncio.to_thread(
                client.get_snapshot_all,
                market_type=SnapshotMarketType.STOCKS,
                tickers=tickers,
            )
            for snap in snapshots:
                price_cache.update(
                    ticker=snap.ticker,
                    price=snap.last_trade.price,
                    timestamp=snap.last_trade.timestamp / 1_000_000_000,  # ns -> s
                )

        await asyncio.sleep(interval)
```

**Suggested poll interval**: default to 15s (safe under the free tier's 5 req/min). This is a fixed constant, not tier-detected — see the rate-limit caveat above.

## Error Handling

The client raises exceptions for HTTP errors:

| Status | Meaning | FinAlly handling |
|--------|---------|-------------------|
| 401 | Invalid API key | Log and fall back to leaving the cache stale; surface in health check |
| 403 | Plan doesn't include this endpoint | Log; same as 401 |
| 429 | Rate limit exceeded | Log; the next scheduled poll will retry — don't retry immediately |
| 5xx | Server error | Client has built-in retry (3 attempts by default) before raising |
| Network error | Connection failure | Log and retry on next interval |

FinAlly's poller (`MassiveDataSource._poll_once`) catches all exceptions per cycle and logs rather than propagating — a single failed poll should never crash the background task or take down the app; the cache simply keeps serving the last known prices until the next successful poll.

## Notes

- **Timestamps**: the Python client's typed models expose `last_trade.timestamp` in **nanoseconds** (raw REST JSON uses nanoseconds for `lastTrade.t`/`lastQuote.t`, milliseconds for aggregate bars `t`). Convert to seconds before storing in FinAlly's `PriceUpdate` (`timestamp = raw_ns / 1_000_000_000`, or `raw_ms / 1_000` for aggregates). This differs from the millisecond timestamps used elsewhere in the API (e.g. aggregate bars) — check units per-endpoint rather than assuming.
- The snapshot endpoint returns **all requested tickers in one call** — critical for staying within rate limits on the free tier; never loop per-ticker calls when polling a watchlist.
- During market-closed hours, `last_trade.price` reflects the last traded price (may include after-hours activity).
- The `day` object resets at market open; during pre-market it may still reflect the previous session.
- `api.polygon.io` and the `polygon-api-client` package continue to work (Massive kept them for backward compatibility), but new code should use `api.massive.com` / the `massive` package — that's what this doc and FinAlly's implementation use throughout.

## Sources

- [API Docs | Massive](https://massive.com/docs)
- [Overview | Stocks REST API - Massive](https://massive.com/docs/rest/stocks/overview)
- [Full Market Snapshot | Stocks REST API - Massive](https://massive.com/docs/rest/stocks/snapshots/full-market-snapshot)
- [Unified Snapshot | Stocks REST API - Massive](https://massive.com/docs/rest/stocks/snapshots/unified-snapshot)
- [Custom Bars | Stocks REST API - Massive](https://massive.com/docs/rest/stocks/aggregates/custom-bars)
- [Previous Day Bar | Stocks REST API - Massive](https://massive.com/docs/rest/stocks/aggregates/previous-day-bar)
- [GitHub - massive-com/client-python](https://github.com/massive-com/client-python)
- [What is the request limit for Massive's RESTful APIs?](https://massive.com/knowledge-base/article/what-is-the-request-limit-for-massives-restful-apis)
- [Massive + Python: Unlocking Real-Time and Historical Stock Market Data](https://massive.com/blog/polygon-io-with-python-for-stock-market-data)
