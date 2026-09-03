# Market Data Interface Design

Unified Python interface for market data in FinAlly. Two implementations — a GBM simulator (default) and a Massive API poller (used when `MASSIVE_API_KEY` is set) — sit behind one abstract interface and write into one shared cache. Everything downstream (SSE streaming, portfolio valuation, trade execution) only ever touches the cache; it is completely agnostic to which source is running.

This document describes the design as implemented in `backend/app/market/` (8 modules, ~500 lines, 73 passing tests — see `planning/MARKET_DATA_SUMMARY.md`). For details of the Massive REST API itself, see `MASSIVE_API.md`. For the simulator's math and correlation model, see `MARKET_SIMULATOR.md`.

## Design Principles

1. **Strategy pattern, one contract.** `SimulatorDataSource` and `MassiveDataSource` both implement `MarketDataSource`. A caller that only knows the abstract interface can't tell which one is running.
2. **Push, not pull.** The interface has no `get_price()` method. Sources push updates into a shared `PriceCache` on their own schedule (every 500ms for the simulator, every 15s for Massive); consumers read from the cache, never from the source directly.
3. **The cache is the single point of truth.** Producers (exactly one at a time — simulator or Massive, chosen at startup) write; consumers (SSE stream, portfolio valuation, trade execution) read. This also means a future multi-consumer or multi-user scenario needs no changes to the data layer.
4. **Selection is an environment-variable switch, made once, at startup.** No runtime toggling between simulator and Massive; the factory reads `MASSIVE_API_KEY` once and returns the appropriate source.

## Core Data Model

```python
from __future__ import annotations
import time
from dataclasses import dataclass, field

@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

`PriceUpdate` is frozen and derives `change`/`change_percent`/`direction` as properties rather than storing them — there is exactly one way to compute them, so storing redundant fields would just be a place for them to drift out of sync with `price`/`previous_price`.

This is the only data structure that leaves the market data layer. Everything downstream — SSE payloads, portfolio math, trade fills — works with `PriceUpdate` objects or their `to_dict()` form.

## Abstract Interface

```python
from abc import ABC, abstractmethod

class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices —
    it reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        # ... app runs ...
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        # ... app shutting down ...
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.

        Starts a background task. Must be called exactly once.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources. Safe to call multiple times."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set and from the PriceCache. No-op if not present."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

Both implementations write to the same shared `PriceCache` passed in at construction — the interface itself never returns a price.

## Price Cache

Thread-safe, in-memory store both sources write to and the SSE streamer (and portfolio/trade code) reads from.

```python
import time
from threading import Lock

class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing; bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Record a new price. First update for a ticker sets previous_price == price (direction='flat')."""
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Shallow-copy snapshot of all current prices."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Bumped on every update — lets SSE detect 'anything changed' in O(1) without diffing."""
        return self._version
```

`version` exists purely so the SSE loop can skip serializing/sending a payload when nothing changed since the last tick, instead of diffing dictionaries every 500ms.

## Factory Function

Selects the data source once, at app startup, based on environment:

```python
import os

def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """MASSIVE_API_KEY set and non-empty -> MassiveDataSource. Otherwise -> SimulatorDataSource.

    Returns an unstarted source — caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        return SimulatorDataSource(price_cache=price_cache)
```

`.strip()` matters: an `.env` file with `MASSIVE_API_KEY=` (present but empty) must fall back to the simulator, not attempt a Massive connection with an empty key.

## Massive Implementation

```python
import asyncio
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

class MassiveDataSource(MarketDataSource):
    """Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call. See MASSIVE_API.md for endpoint details.

    Rate limits: free tier 5 req/min -> poll every 15s (default). Paid tiers
    can poll faster; see MASSIVE_API.md's rate-limit table.
    """

    def __init__(self, api_key: str, price_cache: PriceCache, poll_interval: float = 15.0) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)
        await self._poll_once()  # immediate first poll so the cache isn't empty
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)  # picked up on the next poll cycle

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        if not self._tickers or not self._client:
            return
        try:
            # RESTClient is synchronous -> run in a thread so it doesn't block the event loop
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            for snap in snapshots:
                try:
                    self._cache.update(
                        ticker=snap.ticker,
                        price=snap.last_trade.price,
                        timestamp=snap.last_trade.timestamp / 1_000_000_000,  # ns -> s
                    )
                except (AttributeError, TypeError):
                    continue  # malformed snapshot for this ticker; skip, don't fail the batch
        except Exception:
            pass  # network/HTTP error; log it, retry on the next scheduled interval

    def _fetch_snapshots(self) -> list:
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

Two failure-isolation choices worth calling out:

- A single malformed snapshot (missing `last_trade`, etc.) is skipped without aborting the rest of the batch — one bad ticker shouldn't blank out the other nine.
- A whole failed poll cycle (network error, 429, 401) is swallowed and logged, not raised — the background task must never die from a transient API failure. The cache simply serves stale-but-present prices until the next successful poll.

## Simulator Implementation

See `MARKET_SIMULATOR.md` for the `GBMSimulator` class itself (the price-generation math). The `SimulatorDataSource` wrapper follows the same `MarketDataSource` shape as `MassiveDataSource`:

```python
import asyncio

class SimulatorDataSource(MarketDataSource):
    """Runs a background task that calls GBMSimulator.step() every update_interval
    seconds and writes the results to the PriceCache."""

    def __init__(self, price_cache: PriceCache, update_interval: float = 0.5) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers)
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)  # seed cache before first tick
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    for ticker, price in self._sim.step().items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                pass  # a single bad step shouldn't kill the loop
            await asyncio.sleep(self._interval)
```

Both `start()` implementations seed the cache with an initial price synchronously before the background loop begins — a client connecting to `/api/stream/prices` immediately after startup must not see an empty cache while waiting for the first tick/poll.

## Integration with SSE

The SSE endpoint (`create_stream_router`) reads from the `PriceCache` and pushes to connected clients, using `version` to avoid sending unchanged payloads:

```python
import asyncio, json
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

def create_stream_router(price_cache: PriceCache) -> APIRouter:
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
        )

    return router

async def _generate_events(price_cache: PriceCache, request: Request, interval: float = 0.5):
    yield "retry: 1000\n\n"  # tell EventSource to auto-reconnect after 1s on drop
    last_version = -1
    while True:
        if await request.is_disconnected():
            break
        if price_cache.version != last_version:
            last_version = price_cache.version
            prices = price_cache.get_all()
            if prices:
                data = {ticker: update.to_dict() for ticker, update in prices.items()}
                yield f"data: {json.dumps(data)}\n\n"
        await asyncio.sleep(interval)
```

This polls the cache every 500ms regardless of which source is feeding it — even the Massive poller (updating every 15s) is served through the same 500ms-cadence SSE loop; `version` just means most of those ticks are no-ops until the next real update lands.

## File Structure

```
backend/
  app/
    market/
      __init__.py          # Public exports
      models.py             # PriceUpdate
      interface.py           # MarketDataSource ABC
      cache.py                # PriceCache
      seed_prices.py          # Default seed prices, per-ticker GBM params, correlation groups
      simulator.py             # GBMSimulator + SimulatorDataSource
      massive_client.py        # MassiveDataSource
      factory.py                # create_market_data_source()
      stream.py                  # create_stream_router() — SSE endpoint
```

## Lifecycle

1. **App startup**: create a `PriceCache`, call `create_market_data_source(cache)`, then `await source.start(initial_tickers)` (the default 10 tickers from the watchlist seed data).
2. **Watchlist changes**: `await source.add_ticker(t)` / `await source.remove_ticker(t)` — from the manual watchlist API or from the LLM's `watchlist_changes` actions.
3. **SSE streaming**: `/api/stream/prices` reads `PriceCache.get_all()` every 500ms, gated by `version`.
4. **Trade execution**: reads the current fill price via `PriceCache.get_price(ticker)`.
5. **App shutdown**: `await source.stop()`.

## Usage for Downstream Code

```python
from app.market import PriceCache, create_market_data_source

# Startup
cache = PriceCache()
source = create_market_data_source(cache)  # reads MASSIVE_API_KEY
await source.start(["AAPL", "GOOGL", "MSFT", ...])

# Read prices
update = cache.get("AAPL")          # PriceUpdate or None
price = cache.get_price("AAPL")     # float or None
all_prices = cache.get_all()        # dict[str, PriceUpdate]

# Dynamic watchlist
await source.add_ticker("TSLA")
await source.remove_ticker("GOOGL")

# Shutdown
await source.stop()
```
