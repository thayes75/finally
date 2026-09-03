# Market Data Backend — Detailed Design

**Scope:** everything under `backend/app/market/` — the unified market data API, the GBM
simulator, the Massive (Polygon.io) REST client, the shared price cache, and the SSE
streaming endpoint.

**Status:** this document describes the implemented design. Sections 1–9 and 11–14 match
the code in `backend/app/market/` as built. Section 10 (session-change baseline) is a
designed-but-not-yet-implemented extension needed by the frontend watchlist spec; it is
marked as such.

**Related docs:** `PLAN.md` (§6 Market Data, §8 API Endpoints, §10 Frontend),
`MARKET_DATA_SUMMARY.md` (build summary), `archive/MASSIVE_API.md` (vendor API reference),
`archive/MARKET_DATA_REVIEW.md` (code review).

---

## Table of Contents

1. [Goals and Constraints](#1-goals-and-constraints)
2. [Architecture](#2-architecture)
3. [File Layout](#3-file-layout)
4. [Unified API](#4-unified-api)
5. [The GBM Simulator](#5-the-gbm-simulator)
6. [The Massive API Client](#6-the-massive-api-client)
7. [SSE Streaming Endpoint](#7-sse-streaming-endpoint)
8. [FastAPI Lifecycle Integration](#8-fastapi-lifecycle-integration)
9. [Watchlist Coordination](#9-watchlist-coordination)
10. [Session-Change Baseline (proposed)](#10-session-change-baseline-proposed)
11. [Testing Strategy](#11-testing-strategy)
12. [Error Handling and Edge Cases](#12-error-handling-and-edge-cases)
13. [Configuration Reference](#13-configuration-reference)
14. [Implementation Checklist](#14-implementation-checklist)

---

## 1. Goals and Constraints

| Goal | How it is met |
|---|---|
| Two data sources, one interface | `MarketDataSource` ABC; `SimulatorDataSource` and `MassiveDataSource` implement it |
| Downstream code is source-agnostic | Nothing outside `app/market/` imports a concrete source; everything reads `PriceCache` |
| Zero-config default | No `MASSIVE_API_KEY` → simulator; no network, no account, no setup |
| Live feel at ~500 ms | Simulator ticks at 500 ms; SSE pushes at 500 ms |
| Real data when wanted | Massive REST snapshot polling, one HTTP call per cycle regardless of ticker count |
| Survives failure | Both background loops swallow per-cycle exceptions and keep running |
| Cheap on the free tier | Snapshot endpoint batches all tickers; default poll interval 15 s = 4 req/min < 5 req/min limit |
| Testable without network | Simulator needs no I/O; Massive client is mocked at the `RESTClient` boundary |

Non-goals: order books, limit orders, level-2 quotes, historical bar storage, multi-user
fan-out. The cache is deliberately O(tickers) in memory — latest price only, no history.
Sparklines are accumulated client-side from the SSE stream (PLAN.md §2).

---

## 2. Architecture

```
                     env: MASSIVE_API_KEY
                              │
                   create_market_data_source(cache)
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
    SimulatorDataSource               MassiveDataSource
    (GBM, 500 ms tick)                (REST poll, 15 s)
              │                               │
              └───────────────┬───────────────┘
                              ▼
                  PriceCache  (dict + threading.Lock + version counter)
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
  SSE /api/stream/prices  Portfolio valuation   Trade execution
        │
        ▼
   Browser EventSource → watchlist flashes, sparklines, charts
```

Three rules hold the design together:

1. **Producers push, consumers pull.** A data source never returns a price to a caller; it
   writes into the cache on its own schedule. Consumers read the cache whenever they like.
   This is what makes the two sources interchangeable despite running at wildly different
   cadences (500 ms vs 15 s).
2. **The cache is the only shared mutable state.** It is the single synchronisation point,
   and it is the only thing that needs a lock.
3. **The active ticker set lives in the data source, not the cache.** `add_ticker` /
   `remove_ticker` change what is produced; the cache just holds whatever arrived.

---

## 3. File Layout

```
backend/app/market/
├── __init__.py          # Public API re-exports
├── models.py            # PriceUpdate
├── interface.py         # MarketDataSource ABC
├── cache.py             # PriceCache
├── seed_prices.py       # SEED_PRICES, TICKER_PARAMS, correlation constants
├── simulator.py         # GBMSimulator + SimulatorDataSource
├── massive_client.py    # MassiveDataSource
├── factory.py           # create_market_data_source()
└── stream.py            # create_stream_router() — SSE endpoint

backend/tests/market/
├── test_models.py
├── test_cache.py
├── test_simulator.py            # GBMSimulator math
├── test_simulator_source.py     # SimulatorDataSource async integration
├── test_massive.py              # MassiveDataSource with mocked RESTClient
└── test_factory.py

backend/market_data_demo.py      # Rich terminal dashboard (manual verification)
```

Dependencies (`backend/pyproject.toml`): `fastapi`, `uvicorn[standard]`, `numpy`,
`massive`, `rich`. Dev extras: `pytest`, `pytest-asyncio`, `pytest-cov`, `ruff`.

`massive` is a **core** dependency, not optional. Making it optional would mean lazy
imports, which in turn makes `unittest.mock.patch("...massive_client.RESTClient")` fail
because the module-level name never exists. The package is small and pure-Python, so the
cost of always installing it is lower than the cost of the lazy-import gymnastics.

---

## 4. Unified API

### 4.1 `PriceUpdate` — the only type that leaves the layer

**File: `backend/app/market/models.py`**

```python
"""Data models for market data."""

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
        """Absolute price change from previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous update."""
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

Design decisions:

- **`frozen=True`** — an update is a fact about a moment. Nothing should mutate one after
  the cache hands it out, and freezing means `get_all()` can return a shallow copy safely.
- **`slots=True`** — no `__dict__` per instance. At 2 updates/sec × 10 tickers this is
  ~72 000 objects/hour; slots keeps the allocation cheap.
- **`change` / `change_percent` / `direction` are computed properties, not stored fields.**
  They are pure functions of `price` and `previous_price`, so storing them would be a
  chance to store them inconsistently. `to_dict()` materialises them for the wire.
- **`timestamp` is Unix seconds (float).** Massive returns milliseconds; the client divides
  by 1000 at the boundary so everything inside the layer speaks one unit.

Worked example:

```python
>>> u = PriceUpdate(ticker="AAPL", price=190.55, previous_price=190.00, timestamp=1_759_000_000.0)
>>> u.change
0.55
>>> u.change_percent
0.2895
>>> u.direction
'up'
>>> u.to_dict()
{'ticker': 'AAPL', 'price': 190.55, 'previous_price': 190.0,
 'timestamp': 1759000000.0, 'change': 0.55, 'change_percent': 0.2895, 'direction': 'up'}
```

### 4.2 `PriceCache` — the shared point of truth

**File: `backend/app/market/cache.py`**

```python
"""Thread-safe in-memory price cache."""

from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


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
        """Record a new price for a ticker. Returns the created PriceUpdate.

        Automatically computes direction and change from the previous price.
        If this is the first update for the ticker, previous_price == price (direction='flat').
        """
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
        """Get the latest price for a single ticker, or None if unknown."""
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: get just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        """Remove a ticker from the cache (e.g., when removed from watchlist)."""
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Current version counter. Useful for SSE change detection."""
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

**Why the cache computes `previous_price`, not the caller.** The producer only knows "the
price is now X". Chaining X to the prior value is bookkeeping that belongs in one place;
doing it in the cache means the simulator and the Massive client both get correct
`direction` without either implementing it.

**Why a `threading.Lock` and not an `asyncio.Lock`.** The Massive client calls a
synchronous vendor SDK via `asyncio.to_thread`, so writes genuinely originate from a
worker thread. An `asyncio.Lock` protects only against interleaving inside one event loop
and would not help there. The critical section is a dict get + dict set — contention at
10 tickers × 2 Hz is not measurable.

**Why a version counter.** The SSE generator wakes every 500 ms but should not re-send an
identical payload if nothing was written (which happens whenever the Massive poller is
mid-interval — 29 of every 30 SSE ticks). Comparing one integer is cheaper than diffing a
dict of updates, and the counter is monotonic so a reader can never miss a change by
observing a stale-but-equal value.

`version` reads `self._version` outside the lock. On CPython an aligned `int` load is
atomic under the GIL, and a reader that sees a slightly stale value simply re-checks
500 ms later. Worth revisiting only if the project ever targets a free-threaded build.

### 4.3 `MarketDataSource` — the abstract interface

**File: `backend/app/market/interface.py`**

```python
"""Abstract interface for market data sources."""

from __future__ import annotations

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

        Starts a background task that periodically writes to the PriceCache.
        Must be called exactly once. Calling start() twice is undefined behavior.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources.

        Safe to call multiple times. After stop(), the source will not write
        to the cache again.
        """

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present.

        The next update cycle will include this ticker.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. No-op if not present.

        Also removes the ticker from the PriceCache.
        """

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

Contract notes that both implementations honour:

- `add_ticker` / `remove_ticker` are **idempotent**. Callers (REST route, LLM tool call)
  should not have to check first.
- `stop()` is **safe to call twice** — it guards on `self._task and not self._task.done()`.
  Shutdown paths in FastAPI can fire more than once during reload.
- `add_ticker` / `remove_ticker` are `async` even though the simulator's work is
  synchronous. Keeping them awaitable means a future source can do I/O (subscribe to a
  websocket channel, register a symbol) without changing every call site.
- `get_tickers()` is sync — it is a plain read used by health endpoints and tests.

### 4.4 The factory

**File: `backend/app/market/factory.py`**

```python
"""Factory for creating market data sources."""

from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Create the appropriate market data source based on environment variables.

    - MASSIVE_API_KEY set and non-empty → MassiveDataSource (real market data)
    - Otherwise → SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        logger.info("Market data source: GBM Simulator")
        return SimulatorDataSource(price_cache=price_cache)
```

`.strip()` matters: `.env` files routinely contain `MASSIVE_API_KEY=` or a value with a
trailing space. A whitespace-only key must select the simulator, not attempt a doomed
authenticated call. The factory returns an **unstarted** source so the caller controls
when the first tick happens (the caller has to read the watchlist from SQLite first).

### 4.5 Package exports

**File: `backend/app/market/__init__.py`**

```python
"""Market data subsystem for FinAlly.

Public API:
    PriceUpdate         - Immutable price snapshot dataclass
    PriceCache          - Thread-safe in-memory price store
    MarketDataSource    - Abstract interface for data providers
    create_market_data_source - Factory that selects simulator or Massive
    create_stream_router - FastAPI router factory for SSE endpoint
"""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
]
```

Everything downstream imports from `app.market`, never from `app.market.simulator` or
`app.market.massive_client`. If a route file names a concrete source, the abstraction has
been broken.

---

## 5. The GBM Simulator

### 5.1 The math

Geometric Brownian Motion is the standard continuous-time model for equity prices — it is
what sits under Black–Scholes. Prices evolve multiplicatively, which gives three
properties we want for free: they can never go negative, returns are lognormally
distributed, and volatility scales with price level.

One step:

```
S(t + dt) = S(t) · exp( (μ − σ²/2)·dt  +  σ·√dt·Z )
            └──────────┘ └──────────┘     └──────┘
              current       drift          diffusion
```

| Symbol | Meaning | Example |
|---|---|---|
| `S(t)` | current price | 190.00 |
| `μ` | annualised drift (expected return) | 0.05 → +5 %/yr |
| `σ` | annualised volatility | 0.22 → 22 %/yr |
| `dt` | step size as a fraction of a trading year | ~8.48e-8 |
| `Z` | standard normal draw, correlated across tickers | N(0, 1) |

The `−σ²/2` term is the Itô correction. Without it, `E[S(t)] = S(0)·exp(μt + σ²t/2)` and
high-volatility tickers would drift upward for no economic reason. With it,
`E[S(t)] = S(0)·exp(μt)` — the drift means what it says.

**Choosing `dt`.** A trading year is 252 days × 6.5 hours × 3600 s = **5 896 800 seconds**.
A 500 ms tick is therefore

```
dt = 0.5 / 5_896_800 ≈ 8.48e-8
```

Sanity check on the resulting tick size for AAPL (σ = 0.22, S = 190):

```
σ·√dt = 0.22 × √8.48e-8 = 0.22 × 2.912e-4 ≈ 6.41e-5
one-sigma move ≈ 190 × 6.41e-5 ≈ $0.0122
```

About 1.2 cents per tick, so a visible flash every few ticks and a plausible dollar or two
of drift over a few minutes of watching. Over a full simulated trading day (46 800 ticks)
the accumulated one-sigma range is √46800 × $0.0122 ≈ $2.64 ≈ 1.4 % — which is what
22 % annualised volatility actually implies for a day. The parameters are internally
consistent; the numbers on screen are not arbitrary.

### 5.2 Seed prices and per-ticker parameters

**File: `backend/app/market/seed_prices.py`**

```python
"""Seed prices and per-ticker parameters for the market simulator."""

# Realistic starting prices for the default watchlist (as of project creation)
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters
# sigma: annualized volatility (higher = more price movement)
# mu: annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},  # Low volatility (bank)
    "V": {"sigma": 0.17, "mu": 0.04},  # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

# Default parameters for tickers not in the list above (dynamically added)
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Correlation groups for the simulator's Cholesky decomposition
# Tickers in the same group have higher intra-group correlation
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Correlation coefficients
INTRA_TECH_CORR = 0.6  # Tech stocks move together
INTRA_FINANCE_CORR = 0.5  # Finance stocks move together
CROSS_GROUP_CORR = 0.3  # Between sectors / unknown tickers
TSLA_CORR = 0.3  # TSLA does its own thing
```

The spread of σ from 0.17 (V) to 0.50 (TSLA) is the point: on screen, V ticks along
placidly while TSLA jumps around, and the watchlist reads like a real market instead of
ten copies of the same random walk. A ticker added at runtime that is not in the table
gets `DEFAULT_PARAMS` and a random seed price in `[50, 300)`.

Note `dict(DEFAULT_PARAMS)` is copied per ticker in `_add_ticker_internal`, so a future
per-ticker mutation cannot corrupt the shared default.

### 5.3 Correlated moves via Cholesky decomposition

Independent random walks look wrong: real sectors move together. Given a correlation
matrix `C` (symmetric, unit diagonal, positive-definite), its Cholesky factor `L`
satisfies `L·Lᵀ = C`. If `Z` is a vector of independent standard normals, then `L·Z` has
covariance `L·I·Lᵀ = C` — exactly the correlation structure we asked for, with unit
marginal variances preserved.

The correlation structure:

| Pair | ρ | Rationale |
|---|---|---|
| tech ↔ tech | 0.6 | AAPL/MSFT/NVDA/... move on the same macro news |
| finance ↔ finance | 0.5 | JPM/V track rates and consumer spend |
| TSLA ↔ anything | 0.3 | checked first, so TSLA never gets the 0.6 tech rate |
| cross-sector / unknown | 0.3 | a broad market beta |

The TSLA check runs **before** the sector checks. TSLA is a member of the `tech` set for
grouping purposes but is deliberately excluded from the tech correlation — hence the
ordering in `_pairwise_correlation`. Get that order wrong and TSLA silently becomes just
another tech stock.

The matrix is rebuilt on every add/remove. It is O(n²) to build and O(n³) to factor, but
n < 50 and the operation happens only on watchlist edits, never in the tick loop.

Positive-definiteness holds for this structure: a matrix of the form `(1−ρ)I + ρJ` with
`0 ≤ ρ < 1` has eigenvalues `1 + (n−1)ρ` and `1 − ρ > 0`, and the block-structured variant
used here stays positive-definite for the ρ values chosen. If a future edit raises the
correlations, `np.linalg.cholesky` will raise `LinAlgError` — a loud failure at watchlist-
edit time, which is the right place to find out.

### 5.4 Random shock events

Pure GBM is smooth. PLAN.md asks for "occasional random events — sudden 2–5 % moves on a
ticker for drama". Each ticker, each tick, independently:

```python
if random.random() < self._event_prob:          # 0.001
    shock_magnitude = random.uniform(0.02, 0.05)
    shock_sign = random.choice([-1, 1])
    self._prices[ticker] *= 1 + shock_magnitude * shock_sign
```

Rate arithmetic: 0.001 per ticker-tick × 2 ticks/s × 10 tickers = 0.02 events/s ≈ **one
visible shock somewhere on the board every 50 seconds**. Frequent enough to notice while
demoing, rare enough that a given ticker jumps only about once every 8 minutes.

The shock is applied multiplicatively *after* the GBM step, so it composes cleanly and
cannot drive a price negative.

### 5.5 `GBMSimulator` — the math engine

**File: `backend/app/market/simulator.py` (part 1)**

```python
"""GBM-based market simulator."""

from __future__ import annotations

import asyncio
import logging
import math
import random

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices.

    Math:
        S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)

    Where:
        S(t)   = current price
        mu     = annualized drift (expected return)
        sigma  = annualized volatility
        dt     = time step as fraction of a trading year
        Z      = correlated standard normal random variable

    The tiny dt (~8.5e-8 for 500ms ticks over 252 trading days * 6.5h/day)
    produces sub-cent moves per tick that accumulate naturally over time.
    """

    # 500ms expressed as a fraction of a trading year
    # 252 trading days * 6.5 hours/day * 3600 seconds/hour = 5,896,800 seconds
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability

        # Per-ticker state
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}

        # Cholesky decomposition of the correlation matrix (for correlated moves)
        self._cholesky: np.ndarray | None = None

        # Initialize all starting tickers
        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    # --- Public API ---

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Returns {ticker: new_price}.

        This is the hot path — called every 500ms. Keep it fast.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        # Generate n independent standard normal draws
        z_independent = np.random.standard_normal(n)

        # Apply Cholesky to get correlated draws
        if self._cholesky is not None:
            z_correlated = self._cholesky @ z_independent
        else:
            z_correlated = z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            mu = params["mu"]
            sigma = params["sigma"]

            # GBM: S(t+dt) = S(t) * exp((mu - 0.5*sigma^2)*dt + sigma*sqrt(dt)*Z)
            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random event: ~0.1% chance per tick per ticker
            # With 10 tickers at 2 ticks/sec, expect an event ~every 50 seconds
            if random.random() < self._event_prob:
                shock_magnitude = random.uniform(0.02, 0.05)
                shock_sign = random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock_magnitude * shock_sign
                logger.debug(
                    "Random event on %s: %.1f%% %s",
                    ticker,
                    shock_magnitude * 100,
                    "up" if shock_sign > 0 else "down",
                )

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the simulation. Rebuilds the correlation matrix."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the simulation. Rebuilds the correlation matrix."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        """Current price for a ticker, or None if not tracked."""
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        """Return the list of currently tracked tickers."""
        return list(self._tickers)

    # --- Internals ---

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add a ticker without rebuilding Cholesky (for batch initialization)."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """Rebuild the Cholesky decomposition of the ticker correlation matrix.

        Called whenever tickers are added or removed. O(n^2) but n < 50.
        """
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        # Build the correlation matrix
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = rho
                corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        """Determine correlation between two tickers based on sector grouping.

        Correlation structure:
          - Same tech sector:   0.6
          - Same finance sector: 0.5
          - TSLA with anything: 0.3 (it does its own thing)
          - Cross-sector:       0.3
          - Unknown tickers:    0.3
        """
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        # TSLA is in tech set but behaves independently
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR

        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR

        return CROSS_GROUP_CORR
```

Implementation notes:

- **`self._tickers` is an ordered list, not a set.** Row `i` of the Cholesky factor must
  correspond to `self._tickers[i]`. The list *is* the index mapping; a set would break it.
- **`_add_ticker_internal` vs `add_ticker`.** The constructor adds n tickers and factors
  once. The public method adds one and factors immediately. Splitting them turns an
  O(n·n³) startup into O(n³).
- **`step()` rounds only the returned value**, never `self._prices[ticker]`. Internal state
  stays full-precision so rounding error cannot accumulate over tens of thousands of ticks.
- **`step()` is synchronous and pure-CPU** — no locks, no I/O. It is called from exactly
  one place (the async loop below), so it needs no synchronisation of its own.

### 5.6 `SimulatorDataSource` — the async wrapper

**File: `backend/app/market/simulator.py` (part 2)**

```python
class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by the GBM simulator.

    Runs a background asyncio task that calls GBMSimulator.step() every
    `update_interval` seconds and writes results to the PriceCache.
    """

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(
            tickers=tickers,
            event_probability=self._event_prob,
        )
        # Seed the cache with initial prices so SSE has data immediately
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(tickers))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            # Seed cache immediately so the ticker has a price right away
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
            logger.info("Simulator: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        """Core loop: step the simulation, write to cache, sleep."""
        while True:
            try:
                if self._sim:
                    prices = self._sim.step()
                    for ticker, price in prices.items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

Behaviours worth pointing at:

- **Cache seeding in `start()` and `add_ticker()`.** Without it there would be a 500 ms
  window where the frontend has a watchlist row and no price, and a trade on a
  just-added ticker would 400. Seeding closes the window to zero.
- **`try` is inside the `while`, `await asyncio.sleep` is outside the `try`.** A failing
  step logs and the loop continues at the normal cadence; it cannot spin hot, and a single
  bad tick cannot kill the feed for the session. `logger.exception` captures the traceback.
- **`asyncio.CancelledError` propagates.** It derives from `BaseException` in Python 3.8+,
  so `except Exception` does not swallow it and `stop()` actually stops the loop.
- **`stop()` awaits the cancelled task** rather than firing and forgetting, so shutdown is
  deterministic — by the time `stop()` returns, no further writes can land in the cache.
- **`get_tickers()` delegates to `GBMSimulator.get_tickers()`**, a public method, rather
  than reaching into `_tickers`. Keeps the boundary honest.

---

## 6. The Massive API Client

### 6.1 What we call and why

Massive (formerly Polygon.io) is used through the official `massive` Python SDK. The whole
integration rests on one endpoint:

```
GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,GOOGL,MSFT,...
```

**One HTTP call returns every requested ticker.** That single fact is what makes the free
tier viable: 10 tickers is one request, not ten. Polling every 15 s is 4 requests/minute,
under the 5 req/min free-tier ceiling with a request to spare.

SDK call and the fields we read:

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient(api_key="...")
snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", "MSFT"],
)

for snap in snapshots:
    snap.ticker                  # "AAPL"
    snap.last_trade.price        # 125.07   → PriceCache price
    snap.last_trade.timestamp    # 1675190399000 (Unix ms) → /1000 → seconds
    snap.day.previous_close      # 129.61   → session baseline (see §10)
    snap.day.change_percent      # -3.50    → day change
```

Other endpoints, available but not used by the live feed:
`get_snapshot_ticker()` (single ticker detail view), `get_previous_close_agg()`
(prior-session OHLC, useful for baselines), `list_aggs()` (historical bars, if a
server-side chart history is ever added). See `archive/MASSIVE_API.md` for full request
and response shapes.

### 6.2 Poll interval by tier

| Tier | Rate limit | Recommended `poll_interval` | Requests/min |
|---|---|---|---|
| Free | 5 req/min | **15.0 s** (default) | 4 |
| Paid | effectively unlimited | 2.0–5.0 s | 12–30 |

The default is the safe one. A paid user tightens it by constructing
`MassiveDataSource(..., poll_interval=2.0)`; see §13 for wiring that to an env var.

The asymmetry with the simulator is real and intentional: with Massive, prices change on
screen every 15 s, not every 500 ms. The SSE endpoint still runs at 500 ms and simply
sends nothing when the version counter has not moved, so the frontend needs no knowledge
of which source is active.

### 6.3 `MassiveDataSource`

**File: `backend/app/market/massive_client.py`**

```python
"""Massive (Polygon.io) API client for real market data."""

from __future__ import annotations

import asyncio
import logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call, then writes results to the PriceCache.

    Rate limits:
      - Free tier: 5 req/min → poll every 15s (default)
      - Paid tiers: higher limits → poll every 2-5s
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)

        # Do an immediate first poll so the cache has data right away
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(tickers),
            self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added ticker %s (will appear on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internal ---

    async def _poll_loop(self) -> None:
        """Poll on interval. First poll already happened in start()."""
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """Execute one poll cycle: fetch snapshots, update cache."""
        if not self._tickers or not self._client:
            return

        try:
            # The Massive RESTClient is synchronous — run in a thread to
            # avoid blocking the event loop.
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    # Massive timestamps are Unix milliseconds → convert to seconds
                    timestamp = snap.last_trade.timestamp / 1000.0
                    self._cache.update(
                        ticker=snap.ticker,
                        price=price,
                        timestamp=timestamp,
                    )
                    processed += 1
                except (AttributeError, TypeError) as e:
                    logger.warning(
                        "Skipping snapshot for %s: %s",
                        getattr(snap, "ticker", "???"),
                        e,
                    )
            logger.debug("Massive poll: updated %d/%d tickers", processed, len(self._tickers))

        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise — the loop will retry on the next interval.
            # Common failures: 401 (bad key), 429 (rate limit), network errors.

    def _fetch_snapshots(self) -> list:
        """Synchronous call to the Massive REST API. Runs in a thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

### 6.4 Three details that matter

**`asyncio.to_thread` around the SDK.** `RESTClient` is synchronous `urllib3`. Calling it
directly from a coroutine would block the event loop for the duration of the HTTP
round-trip — during which every SSE connection stalls and every REST request queues. On a
slow response that is a visibly frozen UI. `to_thread` hands it to the default executor and
the loop stays responsive. This is also precisely why `PriceCache` needs a *thread* lock:
`_fetch_snapshots` runs off-loop, and although `self._cache.update(...)` is called back on
the loop thread here, the SDK's own retry threads and the executor boundary make the
thread-safe choice the correct one to encode.

**Two nested `try` blocks, deliberately.** The inner one catches `AttributeError` /
`TypeError` per snapshot — a ticker that is halted, delisted, or has never traded today can
come back with `last_trade = None`. That ticker is skipped and logged; the other nine still
update. The outer one catches transport and auth failures for the whole cycle. Neither
re-raises, because a background task that dies on a transient 429 takes the price feed
down for the rest of the session.

**Immediate first poll in `start()`.** `_poll_loop` sleeps *before* polling, so without the
explicit `await self._poll_once()` in `start()` the app would serve an empty watchlist for
the first 15 seconds. Note this makes `start()` slow (one HTTP round-trip) and able to
surface an invalid key in the logs immediately — both desirable.

**Ticker normalisation.** `add_ticker` / `remove_ticker` apply `.upper().strip()` because
Massive is case-sensitive on symbols and the LLM chat path can produce `"aapl"`. The REST
route should normalise before it writes to SQLite too, so the database and the data source
never disagree (§9).

### 6.5 Simulator vs. Massive at a glance

| | `SimulatorDataSource` | `MassiveDataSource` |
|---|---|---|
| Cadence | 500 ms | 15 s (free) / 2–5 s (paid) |
| I/O | none | one HTTPS call per cycle |
| Prices at `start()` | seeded from `SEED_PRICES` | fetched by an immediate poll |
| Prices on `add_ticker()` | available instantly | available after ≤ 1 poll interval |
| Unknown ticker | invents a price in `[50, 300)` | absent from the response; never enters the cache |
| Failure mode | logged, next tick continues | logged, next poll retries |
| Market hours | always "open" | last trade may be stale / after-hours |

The `add_ticker` gap is the only user-visible behavioural difference, and §12.2 covers how
the trade route handles it.

---

## 7. SSE Streaming Endpoint

**File: `backend/app/market/stream.py`**

```python
"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stream", tags=["streaming"])


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Create the SSE streaming router with a reference to the price cache.

    This factory pattern lets us inject the PriceCache without globals.
    """

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint for live price updates.

        Streams all tracked ticker prices every ~500ms. The client connects
        with EventSource and receives events in the format:

            data: {"AAPL": {"ticker": "AAPL", "price": 190.50, ...}, ...}

        Includes a retry directive so the browser auto-reconnects on
        disconnection (EventSource built-in behavior).
        """
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering if proxied
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    """Async generator that yields SSE-formatted price events.

    Sends all prices every `interval` seconds. Stops when the client
    disconnects (detected via request.is_disconnected()).
    """
    # Tell the client to retry after 1 second if the connection drops
    yield "retry: 1000\n\n"

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            # Check for client disconnect
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()

                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    payload = json.dumps(data)
                    yield f"data: {payload}\n\n"

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
```

### Wire format

```
retry: 1000

data: {"AAPL":{"ticker":"AAPL","price":190.55,"previous_price":190.00,"timestamp":1759000000.5,"change":0.55,"change_percent":0.2895,"direction":"up"},"GOOGL":{...}}

data: {"AAPL":{...},"GOOGL":{...}}

```

Every frame carries the **full** ticker set, not a delta. At 10 tickers × ~140 bytes that
is ~1.4 KB per frame, 2.8 KB/s — trivial, and it makes the client stateless: a reconnecting
`EventSource` is fully caught up on the first frame it receives, with no resync protocol.

The blank line after each `data:` line is required by the SSE spec — it terminates the
event. Hence the `\n\n`.

### Client side

```javascript
const es = new EventSource('/api/stream/prices');

es.onmessage = (event) => {
  const prices = JSON.parse(event.data);          // { AAPL: {...}, GOOGL: {...} }
  for (const [ticker, update] of Object.entries(prices)) {
    applyPrice(ticker, update);                    // flash green/down red, push to sparkline
  }
  setConnectionStatus('connected');                // green dot
};

es.onerror = () => setConnectionStatus('reconnecting');  // yellow dot; EventSource auto-retries
```

`retry: 1000` sets the browser's reconnect delay to 1 s, and `EventSource` handles the
reconnect itself. That covers the "SSE resilience: disconnect and verify reconnection"
E2E scenario in PLAN.md §12.

### Header and design notes

- `Cache-Control: no-cache` — no proxy or browser may cache a stream.
- `X-Accel-Buffering: no` — nginx buffers proxied responses by default, which would hold
  events until the buffer filled and destroy the real-time feel. Set proactively for any
  deployment that puts the container behind nginx.
- **Polling the cache rather than being notified by the source.** Frames are evenly spaced
  at 500 ms regardless of which source is running, which is exactly what the frontend
  sparklines need — irregular spacing produces a distorted chart. It also decouples the SSE
  layer from the producers completely.
- **`create_stream_router` closes over the cache** rather than reading a module global, so
  a test can build a router over its own cache instance.

  *Known footgun:* `router` is a module-level `APIRouter`, so calling
  `create_stream_router()` twice registers `/prices` twice on the same router object. It is
  called once at startup, so this is latent rather than live. The fix, if a test ever needs
  two routers, is to move `router = APIRouter(...)` inside the factory function.

---

## 8. FastAPI Lifecycle Integration

The cache and the data source are created at app startup and torn down at shutdown via the
`lifespan` context manager, and stored on `app.state` so routes can reach them through
dependency injection.

**File: `backend/app/main.py` (market-data portions)**

```python
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Request

from app.market import (
    MarketDataSource,
    PriceCache,
    create_market_data_source,
    create_stream_router,
)

DEFAULT_TICKERS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA",
                   "NVDA", "META", "JPM", "V", "NFLX"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start and stop the market data subsystem with the app."""

    # --- STARTUP ---
    price_cache = PriceCache()
    app.state.price_cache = price_cache

    source = create_market_data_source(price_cache)   # reads MASSIVE_API_KEY
    app.state.market_source = source

    # Initial ticker set = watchlist from SQLite, plus anything we hold a position in
    tickers = await load_tracked_tickers()            # see §9
    await source.start(tickers or DEFAULT_TICKERS)

    yield  # ---- app serves requests ----

    # --- SHUTDOWN ---
    await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)

# The SSE router needs the cache, so build it at import time against app.state
# via the dependency below, or include it inside lifespan once the cache exists.
```

**Router registration.** `app.include_router()` must run before the first request is
served. Two workable options:

```python
# Option A — register inside lifespan, before `yield` (works; routes are added
# during startup, prior to the app accepting traffic)
app.include_router(create_stream_router(price_cache))

# Option B — hoist cache creation to module scope so the router can be built
# at import time. Simpler to reason about; the cache has no startup cost.
price_cache = PriceCache()
app = FastAPI(title="FinAlly", lifespan=lifespan)
app.include_router(create_stream_router(price_cache))
```

Option B is recommended: constructing a `PriceCache` is just a dict and a lock, there is
nothing to defer, and it keeps route registration in the ordinary place. `lifespan` then
only starts and stops the *source*.

**Dependencies for other routes:**

```python
def get_price_cache(request: Request) -> PriceCache:
    return request.app.state.price_cache


def get_market_source(request: Request) -> MarketDataSource:
    return request.app.state.market_source
```

Taking `request` as a parameter (rather than closing over the module-level `app`) keeps
these usable under `TestClient` with a freshly-built app.

**Usage in the trade route:**

```python
@router.post("/api/portfolio/trade")
async def execute_trade(
    trade: TradeRequest,
    price_cache: PriceCache = Depends(get_price_cache),
):
    price = price_cache.get_price(trade.ticker)
    if price is None:
        raise HTTPException(
            status_code=400,
            detail=f"Price not yet available for {trade.ticker}. Please try again in a moment.",
        )
    # ... validate cash / shares, insert into trades, upsert positions, snapshot ...
```

**Usage in portfolio valuation:**

```python
def value_portfolio(positions: list[Position], cash: float, cache: PriceCache) -> dict:
    prices = cache.get_all()                     # one snapshot, consistent across the loop
    total_positions_value = 0.0
    rows = []
    for pos in positions:
        update = prices.get(pos.ticker)
        current = update.price if update else pos.avg_cost   # fall back to cost basis
        market_value = current * pos.quantity
        unrealized = (current - pos.avg_cost) * pos.quantity
        total_positions_value += market_value
        rows.append({
            "ticker": pos.ticker,
            "quantity": pos.quantity,
            "avg_cost": pos.avg_cost,
            "current_price": current,
            "market_value": round(market_value, 2),
            "unrealized_pnl": round(unrealized, 2),
            "unrealized_pnl_percent": round(
                (current - pos.avg_cost) / pos.avg_cost * 100, 2
            ) if pos.avg_cost else 0.0,
        })
    return {
        "cash_balance": round(cash, 2),
        "positions": rows,
        "total_value": round(cash + total_positions_value, 2),
    }
```

Call `get_all()` **once** and index into the result. Calling `cache.get(...)` inside the
loop takes and releases the lock per position and can observe prices from different ticks,
so the total would not equal the sum of its parts.

**Health check exposing the market data state:**

```python
@app.get("/api/health")
async def health(
    cache: PriceCache = Depends(get_price_cache),
    source: MarketDataSource = Depends(get_market_source),
):
    return {
        "status": "ok",
        "market_source": type(source).__name__,   # SimulatorDataSource | MassiveDataSource
        "tracked_tickers": len(source.get_tickers()),
        "cached_prices": len(cache),
    }
```

**Portfolio snapshot background task** (PLAN.md §7 — every 30 s and after each trade) is a
separate consumer of the same cache; start it alongside the source in `lifespan`:

```python
async def snapshot_loop(cache: PriceCache, interval: float = 30.0) -> None:
    while True:
        try:
            await record_portfolio_snapshot(cache)   # writes portfolio_snapshots row
        except Exception:
            logger.exception("Portfolio snapshot failed")
        await asyncio.sleep(interval)
```

Same shape as the price loops: guard the body, sleep outside the guard, cancel on shutdown.

---

## 9. Watchlist Coordination

Two stores must agree on the ticker set: the `watchlist` table in SQLite (durable) and the
data source's active set (in-memory). The REST routes are the only place they are written,
so that is where they are kept in sync.

### Adding

```
POST /api/watchlist {"ticker": "pypl"}
  → normalise → "PYPL"
  → INSERT INTO watchlist (id, user_id, ticker, added_at)      # UNIQUE(user_id, ticker)
  → await source.add_ticker("PYPL")
        Simulator: seeds a price, rebuilds Cholesky, cache populated immediately
        Massive:   appended to the poll list, price arrives within one interval
  → 200 {"ticker": "PYPL", "price": <float | null>}
```

```python
@router.post("/api/watchlist")
async def add_to_watchlist(
    payload: WatchlistAdd,
    source: MarketDataSource = Depends(get_market_source),
    cache: PriceCache = Depends(get_price_cache),
):
    ticker = payload.ticker.upper().strip()
    if not ticker.isalpha() or len(ticker) > 5:
        raise HTTPException(400, f"Invalid ticker: {payload.ticker!r}")

    await db.add_watchlist_entry(ticker)   # idempotent via UNIQUE(user_id, ticker)
    await source.add_ticker(ticker)        # idempotent by contract

    return {"ticker": ticker, "price": cache.get_price(ticker)}
```

Normalise **once**, at the edge, and use the normalised value for both writes. The LLM chat
path (`watchlist_changes` in the structured output) must call this same function rather
than duplicating the logic.

### Removing — and the position edge case

A ticker removed from the watchlist but still **held** must keep streaming, or portfolio
valuation silently falls back to cost basis and P&L freezes.

```python
@router.delete("/api/watchlist/{ticker}")
async def remove_from_watchlist(
    ticker: str,
    source: MarketDataSource = Depends(get_market_source),
):
    ticker = ticker.upper().strip()
    await db.delete_watchlist_entry(ticker)

    # Keep tracking if we still hold shares — valuation needs a live price
    position = await db.get_position(ticker)
    if position is None or position.quantity == 0:
        await source.remove_ticker(ticker)

    return {"status": "ok", "ticker": ticker}
```

The mirror of this rule applies after a **sell that closes a position**: if the ticker is
also no longer on the watchlist, it can be dropped from the source. If it is still on the
watchlist, it stays. Encoding the rule once avoids getting it wrong in two places:

```python
async def should_track(ticker: str) -> bool:
    """A ticker is tracked if it is on the watchlist OR we hold a position in it."""
    return await db.is_on_watchlist(ticker) or await db.has_open_position(ticker)


async def reconcile_ticker(ticker: str, source: MarketDataSource) -> None:
    """Bring the data source in line with the database for one ticker."""
    if await should_track(ticker):
        await source.add_ticker(ticker)
    else:
        await source.remove_ticker(ticker)
```

Call `reconcile_ticker` after any watchlist mutation and after any trade that opens or
closes a position. Both source methods are idempotent, so calling it redundantly is free.

### Startup: the same rule

```python
async def load_tracked_tickers() -> list[str]:
    """Union of watchlist tickers and tickers with open positions."""
    watchlist = await db.get_watchlist_tickers()      # seeded with the default 10
    held = await db.get_position_tickers()
    return sorted(set(watchlist) | set(held))
```

---

## 10. Session-Change Baseline (proposed)

**Status: designed, not yet implemented.** Everything above this section describes shipped
code; this section is a gap to close before the frontend watchlist is built.

**The gap.** PLAN.md §10 requires the watchlist panel to show "daily change %".
`PriceUpdate.change_percent` is **tick-to-tick**, not session-to-date: at 500 ms ticks it
is a number like `0.0002 %` that flickers around zero. It is exactly right for driving the
green/red flash animation and exactly wrong as the "change" column. Meanwhile Massive
already returns `day.previous_close` and `day.change_percent` on every snapshot, and we
currently discard both.

**The design.** Give the cache a per-ticker **baseline** — the reference price the session
change is measured against — and expose it on `PriceUpdate`.

Additions to `models.py`:

```python
@dataclass(frozen=True, slots=True)
class PriceUpdate:
    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)
    baseline: float | None = None      # session/previous-close reference price

    # ... change / change_percent / direction unchanged (tick-to-tick, drives the flash) ...

    @property
    def day_change(self) -> float:
        """Absolute change from the session baseline. 0.0 if no baseline known."""
        if not self.baseline:
            return 0.0
        return round(self.price - self.baseline, 4)

    @property
    def day_change_percent(self) -> float:
        """Percent change from the session baseline. 0.0 if no baseline known."""
        if not self.baseline:
            return 0.0
        return round((self.price - self.baseline) / self.baseline * 100, 4)

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,                        # tick-to-tick → flash
            "change_percent": self.change_percent,
            "direction": self.direction,
            "baseline": self.baseline,
            "day_change": self.day_change,                # session → watchlist column
            "day_change_percent": self.day_change_percent,
        }
```

Additions to `cache.py`:

```python
class PriceCache:
    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._baselines: dict[str, float] = {}
        self._lock = Lock()
        self._version: int = 0

    def set_baseline(self, ticker: str, baseline: float) -> None:
        """Set the session reference price (seed price, or previous close)."""
        with self._lock:
            self._baselines[ticker] = baseline

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
        baseline: float | None = None,
    ) -> PriceUpdate:
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            if baseline is not None:
                self._baselines[ticker] = baseline
            # First price for a ticker doubles as its baseline if none was given
            resolved = self._baselines.setdefault(ticker, round(price, 2))

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
                baseline=resolved,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def remove(self, ticker: str) -> None:
        with self._lock:
            self._prices.pop(ticker, None)
            self._baselines.pop(ticker, None)
```

Wiring in the two sources:

```python
# SimulatorDataSource.start() and .add_ticker() — the seed price is the baseline
price = self._sim.get_price(ticker)
if price is not None:
    self._cache.set_baseline(ticker, round(price, 2))
    self._cache.update(ticker=ticker, price=price)
```

```python
# MassiveDataSource._poll_once() — the real previous close is the baseline
self._cache.update(
    ticker=snap.ticker,
    price=snap.last_trade.price,
    timestamp=snap.last_trade.timestamp / 1000.0,
    baseline=getattr(snap.day, "previous_close", None) or None,
)
```

Cost: two extra JSON fields per ticker per frame (~30 bytes), one extra dict. The
`setdefault` fallback means the simulator gets a sensible baseline (its seed price) even if
`set_baseline` is never called, and the Massive path overwrites it with the real previous
close on the first poll. No downstream code breaks — the existing fields keep their exact
meanings.

Tests to add alongside: `day_change_percent` is 0.0 with no baseline; a baseline set once
survives many `update()` calls; `remove()` clears the baseline so a re-added ticker
re-baselines.

---

## 11. Testing Strategy

73 tests across 6 modules, 84 % coverage on `app/market/`. `pyproject.toml` sets
`asyncio_mode = "auto"`, so async tests need no `@pytest.mark.asyncio` decorator.

```bash
cd backend
uv sync --extra dev
uv run --extra dev pytest -v                     # all tests
uv run --extra dev pytest --cov=app --cov-report=term-missing
uv run --extra dev ruff check app/ tests/
```

### 11.1 `GBMSimulator` — deterministic assertions about a random process

The trick is to assert on *invariants* and *statistics*, never on specific prices.

```python
import numpy as np
from app.market.simulator import GBMSimulator
from app.market.seed_prices import SEED_PRICES


def test_seed_prices_used():
    sim = GBMSimulator(tickers=["AAPL", "TSLA"])
    assert sim.get_price("AAPL") == SEED_PRICES["AAPL"]
    assert sim.get_price("TSLA") == SEED_PRICES["TSLA"]


def test_unknown_ticker_gets_random_seed_in_range():
    sim = GBMSimulator(tickers=["ZZZZ"])
    assert 50.0 <= sim.get_price("ZZZZ") < 300.0


def test_prices_stay_positive_over_many_steps():
    sim = GBMSimulator(tickers=list(SEED_PRICES), event_probability=0.05)
    for _ in range(2000):
        for price in sim.step().values():
            assert price > 0


def test_step_moves_prices_but_not_wildly():
    """No shocks: 1000 ticks should not move AAPL by more than a few percent."""
    sim = GBMSimulator(tickers=["AAPL"], event_probability=0.0)
    start = sim.get_price("AAPL")
    for _ in range(1000):
        sim.step()
    drift = abs(sim.get_price("AAPL") - start) / start
    assert drift < 0.05          # ~7 sigma for 1000 ticks at sigma=0.22


def test_step_returns_all_tickers_rounded():
    sim = GBMSimulator(tickers=["AAPL", "MSFT", "JPM"])
    prices = sim.step()
    assert set(prices) == {"AAPL", "MSFT", "JPM"}
    assert all(round(p, 2) == p for p in prices.values())


def test_empty_simulator_steps_cleanly():
    assert GBMSimulator(tickers=[]).step() == {}


def test_cholesky_none_for_single_ticker():
    assert GBMSimulator(tickers=["AAPL"])._cholesky is None


def test_cholesky_valid_for_full_default_watchlist():
    """The 10-ticker correlation matrix must be positive-definite."""
    tickers = list(SEED_PRICES)
    sim = GBMSimulator(tickers=tickers)
    L = sim._cholesky
    assert L.shape == (10, 10)
    reconstructed = L @ L.T
    for i, t1 in enumerate(tickers):
        assert np.isclose(reconstructed[i, i], 1.0)
        for j, t2 in enumerate(tickers):
            if i != j:
                assert np.isclose(
                    reconstructed[i, j], GBMSimulator._pairwise_correlation(t1, t2)
                )


def test_tsla_correlation_beats_tech_membership():
    assert GBMSimulator._pairwise_correlation("TSLA", "AAPL") == 0.3
    assert GBMSimulator._pairwise_correlation("AAPL", "MSFT") == 0.6
    assert GBMSimulator._pairwise_correlation("JPM", "V") == 0.5
    assert GBMSimulator._pairwise_correlation("AAPL", "JPM") == 0.3


def test_add_remove_ticker_rebuilds_matrix():
    sim = GBMSimulator(tickers=["AAPL", "MSFT"])
    sim.add_ticker("NVDA")
    assert sim.get_tickers() == ["AAPL", "MSFT", "NVDA"]
    assert sim._cholesky.shape == (3, 3)
    sim.remove_ticker("MSFT")
    assert sim.get_tickers() == ["AAPL", "NVDA"]
    assert sim._cholesky.shape == (2, 2)
    assert sim.get_price("MSFT") is None


def test_add_and_remove_are_idempotent():
    sim = GBMSimulator(tickers=["AAPL"])
    sim.add_ticker("AAPL")
    sim.remove_ticker("NOPE")
    assert sim.get_tickers() == ["AAPL"]


def test_correlated_moves_are_actually_correlated():
    """AAPL/MSFT (rho=0.6) should co-move more than AAPL/JPM (rho=0.3)."""
    sim = GBMSimulator(tickers=["AAPL", "MSFT", "JPM"], event_probability=0.0)
    prev = dict(sim._prices)
    rets = {"AAPL": [], "MSFT": [], "JPM": []}
    for _ in range(3000):
        sim.step()
        for t in rets:
            rets[t].append(sim._prices[t] / prev[t] - 1)
        prev = dict(sim._prices)
    same = np.corrcoef(rets["AAPL"], rets["MSFT"])[0, 1]
    cross = np.corrcoef(rets["AAPL"], rets["JPM"])[0, 1]
    assert same > cross
```

### 11.2 `PriceCache`

```python
from concurrent.futures import ThreadPoolExecutor
from app.market.cache import PriceCache


def test_first_update_is_flat():
    cache = PriceCache()
    u = cache.update("AAPL", 190.00)
    assert u.previous_price == 190.00 and u.direction == "flat" and u.change == 0.0


def test_direction_and_change_chain_across_updates():
    cache = PriceCache()
    cache.update("AAPL", 190.00)
    up = cache.update("AAPL", 191.00)
    assert up.direction == "up" and up.previous_price == 190.00 and up.change == 1.0
    down = cache.update("AAPL", 189.50)
    assert down.direction == "down" and down.previous_price == 191.00


def test_prices_are_rounded_to_cents():
    cache = PriceCache()
    assert cache.update("AAPL", 190.123456).price == 190.12


def test_version_increments_per_update():
    cache = PriceCache()
    assert cache.version == 0
    cache.update("AAPL", 190.0)
    cache.update("MSFT", 420.0)
    assert cache.version == 2


def test_get_all_returns_a_copy():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    snapshot = cache.get_all()
    cache.update("MSFT", 420.0)
    assert "MSFT" not in snapshot          # snapshot was not mutated behind our back


def test_misses_and_removal():
    cache = PriceCache()
    assert cache.get("NOPE") is None and cache.get_price("NOPE") is None
    cache.update("AAPL", 190.0)
    assert "AAPL" in cache and len(cache) == 1
    cache.remove("AAPL")
    cache.remove("AAPL")                   # idempotent
    assert "AAPL" not in cache and len(cache) == 0


def test_concurrent_writers_do_not_lose_updates():
    cache = PriceCache()
    tickers = [f"T{i}" for i in range(8)]

    def hammer(ticker: str) -> None:
        for n in range(500):
            cache.update(ticker, 100.0 + n * 0.01)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(hammer, tickers))

    assert cache.version == 8 * 500
    assert len(cache) == 8
```

### 11.3 `SimulatorDataSource` — async integration

```python
import asyncio
from app.market.cache import PriceCache
from app.market.simulator import SimulatorDataSource


async def test_start_seeds_cache_immediately():
    cache = PriceCache()
    source = SimulatorDataSource(price_cache=cache, update_interval=0.01)
    await source.start(["AAPL", "MSFT"])
    try:
        assert cache.get_price("AAPL") is not None   # before any tick elapsed
        assert cache.get_price("MSFT") is not None
    finally:
        await source.stop()


async def test_loop_writes_repeatedly():
    cache = PriceCache()
    source = SimulatorDataSource(price_cache=cache, update_interval=0.01)
    await source.start(["AAPL"])
    try:
        await asyncio.sleep(0.1)
        assert cache.version > 3
    finally:
        await source.stop()


async def test_stop_halts_writes_and_is_idempotent():
    cache = PriceCache()
    source = SimulatorDataSource(price_cache=cache, update_interval=0.01)
    await source.start(["AAPL"])
    await asyncio.sleep(0.05)
    await source.stop()
    await source.stop()                     # second call must not raise
    frozen = cache.version
    await asyncio.sleep(0.05)
    assert cache.version == frozen


async def test_add_and_remove_ticker_reach_the_cache():
    cache = PriceCache()
    source = SimulatorDataSource(price_cache=cache, update_interval=0.01)
    await source.start(["AAPL"])
    try:
        await source.add_ticker("NVDA")
        assert cache.get_price("NVDA") is not None      # seeded, no wait needed
        assert "NVDA" in source.get_tickers()
        await source.remove_ticker("NVDA")
        assert cache.get("NVDA") is None
        assert "NVDA" not in source.get_tickers()
    finally:
        await source.stop()


async def test_empty_ticker_list_is_harmless():
    cache = PriceCache()
    source = SimulatorDataSource(price_cache=cache, update_interval=0.01)
    await source.start([])
    try:
        await asyncio.sleep(0.05)
        assert len(cache) == 0
    finally:
        await source.stop()
```

### 11.4 `MassiveDataSource` — mocked at the SDK boundary

Never hit the network in tests. Patch `RESTClient` where it is *used*
(`app.market.massive_client.RESTClient`), not where it is defined. This works precisely
because `massive_client.py` imports it at module level.

```python
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource


def fake_snapshot(ticker: str, price: float, ts_ms: int = 1_675_190_399_000):
    return SimpleNamespace(
        ticker=ticker,
        last_trade=SimpleNamespace(price=price, size=100, timestamp=ts_ms),
        day=SimpleNamespace(previous_close=price * 0.99, change_percent=1.0),
    )


async def test_poll_writes_every_snapshot_to_cache():
    cache = PriceCache()
    source = MassiveDataSource(api_key="k", price_cache=cache)
    source._client = MagicMock()
    source._tickers = ["AAPL", "MSFT"]
    source._client.get_snapshot_all.return_value = [
        fake_snapshot("AAPL", 190.55),
        fake_snapshot("MSFT", 420.10),
    ]

    await source._poll_once()

    assert cache.get_price("AAPL") == 190.55
    assert cache.get_price("MSFT") == 420.10


async def test_timestamps_convert_ms_to_seconds():
    cache = PriceCache()
    source = MassiveDataSource(api_key="k", price_cache=cache)
    source._client = MagicMock()
    source._tickers = ["AAPL"]
    source._client.get_snapshot_all.return_value = [fake_snapshot("AAPL", 190.0, 1_675_190_399_000)]

    await source._poll_once()

    assert cache.get("AAPL").timestamp == 1_675_190_399.0


async def test_malformed_snapshot_is_skipped_not_fatal():
    """A halted ticker with last_trade=None must not stop the others."""
    cache = PriceCache()
    source = MassiveDataSource(api_key="k", price_cache=cache)
    source._client = MagicMock()
    source._tickers = ["BAD", "AAPL"]
    source._client.get_snapshot_all.return_value = [
        SimpleNamespace(ticker="BAD", last_trade=None, day=None),
        fake_snapshot("AAPL", 190.55),
    ]

    await source._poll_once()

    assert cache.get("BAD") is None
    assert cache.get_price("AAPL") == 190.55


async def test_api_error_is_swallowed_so_the_loop_survives():
    cache = PriceCache()
    source = MassiveDataSource(api_key="k", price_cache=cache)
    source._client = MagicMock()
    source._tickers = ["AAPL"]
    source._client.get_snapshot_all.side_effect = RuntimeError("429 rate limited")

    await source._poll_once()          # must not raise

    assert len(cache) == 0


async def test_poll_is_a_noop_with_no_tickers():
    source = MassiveDataSource(api_key="k", price_cache=PriceCache())
    source._client = MagicMock()
    source._tickers = []
    await source._poll_once()
    source._client.get_snapshot_all.assert_not_called()


@patch("app.market.massive_client.RESTClient")
async def test_start_polls_immediately_then_schedules_loop(mock_client_cls):
    cache = PriceCache()
    mock_client_cls.return_value.get_snapshot_all.return_value = [fake_snapshot("AAPL", 190.0)]
    source = MassiveDataSource(api_key="k", price_cache=cache, poll_interval=60.0)

    await source.start(["AAPL"])
    try:
        assert cache.get_price("AAPL") == 190.0     # before the 60s interval elapsed
    finally:
        await source.stop()
    assert source._task is None and source._client is None


async def test_ticker_normalisation():
    source = MassiveDataSource(api_key="k", price_cache=PriceCache())
    await source.add_ticker(" aapl ")
    assert source.get_tickers() == ["AAPL"]
    await source.add_ticker("AAPL")                 # idempotent
    assert source.get_tickers() == ["AAPL"]
    await source.remove_ticker("aapl")
    assert source.get_tickers() == []
```

### 11.5 Factory

```python
from unittest.mock import patch

from app.market.cache import PriceCache
from app.market.factory import create_market_data_source
from app.market.massive_client import MassiveDataSource
from app.market.simulator import SimulatorDataSource


@patch.dict("os.environ", {}, clear=True)
def test_no_key_selects_simulator():
    assert isinstance(create_market_data_source(PriceCache()), SimulatorDataSource)


@patch.dict("os.environ", {"MASSIVE_API_KEY": ""})
def test_empty_key_selects_simulator():
    assert isinstance(create_market_data_source(PriceCache()), SimulatorDataSource)


@patch.dict("os.environ", {"MASSIVE_API_KEY": "   "})
def test_whitespace_key_selects_simulator():
    assert isinstance(create_market_data_source(PriceCache()), SimulatorDataSource)


@patch.dict("os.environ", {"MASSIVE_API_KEY": "real-key"})
def test_key_selects_massive():
    source = create_market_data_source(PriceCache())
    assert isinstance(source, MassiveDataSource)
    assert source.get_tickers() == []      # unstarted
```

### 11.6 SSE endpoint (gap to close)

`stream.py` sits at 31 % coverage with no dedicated test. One integration test is worth
adding, using `httpx.ASGITransport` to read a couple of frames:

```python
import json

import httpx
from fastapi import FastAPI

from app.market.cache import PriceCache
from app.market.stream import create_stream_router


async def test_sse_emits_retry_then_price_frames():
    cache = PriceCache()
    cache.update("AAPL", 190.00)

    app = FastAPI()
    app.include_router(create_stream_router(cache))

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        async with client.stream("GET", "/api/stream/prices") as response:
            assert response.status_code == 200
            assert response.headers["content-type"].startswith("text/event-stream")

            chunks = []
            async for chunk in response.aiter_text():
                chunks.append(chunk)
                if len(chunks) >= 2:
                    break

    body = "".join(chunks)
    assert body.startswith("retry: 1000\n\n")
    payload = json.loads(body.split("data: ", 1)[1].split("\n\n", 1)[0])
    assert payload["AAPL"]["price"] == 190.00
    assert payload["AAPL"]["direction"] == "flat"
```

(Because `router` is module-level, run this in its own module or move the `APIRouter`
construction inside `create_stream_router` first — see §7.)

### 11.7 Manual verification

```bash
cd backend && uv run market_data_demo.py
```

A Rich terminal dashboard: all 10 tickers, live prices, unicode sparklines, coloured
direction arrows, and an event log for notable moves. Runs 60 s or until Ctrl-C. This is
the fastest way to eyeball whether the volatility parameters and shock rate *feel* right —
something no assertion captures.

---

## 12. Error Handling and Edge Cases

**12.1 Empty watchlist at startup.** `start([])` is valid. The simulator builds a zero-
ticker `GBMSimulator` whose `step()` returns `{}`; the Massive poller returns early without
an API call. The SSE endpoint sends no frames (the `if prices:` guard). The first
`add_ticker` brings the system to life. No special-casing needed anywhere.

**12.2 Trade on a ticker with no cached price.** Only reachable under Massive, in the
window between `add_ticker` and the next poll. Return a 400 with a message the UI (and the
LLM) can relay:

```python
price = cache.get_price(ticker)
if price is None:
    raise HTTPException(400, f"Price not yet available for {ticker}. Please try again in a moment.")
```

For the LLM path this string is what lands in the chat response, so it must read as an
explanation to a person, not a stack trace. The simulator never hits this because
`add_ticker` seeds the cache synchronously.

**12.3 Invalid or expired Massive API key.** The immediate poll in `start()` raises inside
`_poll_once`, is caught and logged at ERROR, and the loop keeps retrying every interval.
The app starts, SSE connects, and the UI shows a green dot with no prices — technically
accurate but confusing. Mitigation: surface the source and cache depth on `/api/health`
(§8) so `"cached_prices": 0` with `"market_source": "MassiveDataSource"` is diagnosable at
a glance. A future improvement is a `last_successful_poll` timestamp on the source, shown
in the header when it goes stale.

**12.4 Rate limit (429).** Caught by the same handler; the next poll happens on schedule.
Prices go stale by one interval and recover on their own. The default 15 s interval leaves
1 req/min of headroom on the free tier specifically so a retry cannot cascade.

**12.5 Halted, delisted, or never-traded tickers.** Massive returns a snapshot with
`last_trade = None`. The inner `except (AttributeError, TypeError)` skips that entry and
logs a warning; every other ticker in the batch still updates. A ticker that never
resolves simply never enters the cache — the watchlist row renders with a placeholder
price and any trade on it 400s per §12.2.

**12.6 A ticker that does not exist at all.** The simulator will happily invent a price for
`"NOTREAL"` — it is a simulator. Massive will silently omit it. Validate at the REST edge
(`isalpha()`, length ≤ 5) so obvious garbage is rejected before it reaches either source;
the two sources' divergent behaviour on plausible-but-nonexistent symbols is accepted.

**12.7 Client disconnects mid-stream.** `request.is_disconnected()` is checked at the top
of each loop iteration, so the generator exits within one 500 ms interval and the task is
freed. `asyncio.CancelledError` is also caught and logged for the case where the server
tears the connection down first.

**12.8 Concurrent SSE clients.** Each connection gets its own generator with its own
`last_version`; they share only reads of the cache. N clients cost N × (one dict copy +
one `json.dumps`) per interval. Fine for the single-user design and for a classroom demo
with a handful of tabs open.

**12.9 Numerical stability of the simulator.** Prices cannot go negative (multiplicative
`exp`), cannot drift from rounding (rounding is applied to the returned value only, never
to internal state), and cannot overflow at realistic parameters — a 100× move would need
roughly 10⁸ ticks of one-directional luck.

**12.10 Restart behaviour.** Nothing about market data is persisted. On restart the
simulator re-seeds from `SEED_PRICES`, so prices visibly jump back to their starting
values while positions and cash (in SQLite) carry over. This is a deliberate simplification
consistent with PLAN.md; persisting the last simulated price per ticker would be a small
future change (a `sim_prices` table read in `start()`).

---

## 13. Configuration Reference

| Parameter | Where | Default | Effect |
|---|---|---|---|
| `MASSIVE_API_KEY` | env / `.env` | `""` | Non-empty → Massive; empty/absent → simulator |
| `poll_interval` | `MassiveDataSource.__init__` | `15.0` s | Massive poll cadence (4 req/min) |
| `update_interval` | `SimulatorDataSource.__init__` | `0.5` s | Simulator tick cadence |
| `event_probability` | `GBMSimulator.__init__` | `0.001` | Shock chance per ticker per tick |
| `dt` | `GBMSimulator.__init__` | `~8.48e-8` | GBM step as a fraction of a trading year |
| `interval` | `_generate_events()` | `0.5` s | SSE frame cadence |
| retry directive | `_generate_events()` | `1000` ms | Browser `EventSource` reconnect delay |
| `SEED_PRICES` | `seed_prices.py` | 10 tickers | Simulator starting prices |
| `TICKER_PARAMS` | `seed_prices.py` | per ticker | σ and μ per ticker |
| `DEFAULT_PARAMS` | `seed_prices.py` | σ=0.25, μ=0.05 | Params for unlisted tickers |
| correlation constants | `seed_prices.py` | 0.6 / 0.5 / 0.3 | Tech / finance / cross-sector ρ |

Only `MASSIVE_API_KEY` is externally configurable today; the rest are constructor defaults.
If paid-tier users need a faster poll, the smallest change is to read one more env var in
the factory:

```python
poll_interval = float(os.environ.get("MASSIVE_POLL_INTERVAL", "15"))
return MassiveDataSource(api_key=api_key, price_cache=price_cache, poll_interval=poll_interval)
```

Document it in `.env.example` alongside `MASSIVE_API_KEY` if added.

---

## 14. Implementation Checklist

Built and tested:

- [x] `models.py` — `PriceUpdate` (frozen, slots, computed change/direction, `to_dict`)
- [x] `cache.py` — `PriceCache` (thread-safe, version counter)
- [x] `interface.py` — `MarketDataSource` ABC
- [x] `seed_prices.py` — seed prices, per-ticker σ/μ, correlation constants
- [x] `simulator.py` — `GBMSimulator` (GBM + Cholesky + shocks) and `SimulatorDataSource`
- [x] `massive_client.py` — `MassiveDataSource` (snapshot polling, thread offload)
- [x] `factory.py` — env-driven source selection
- [x] `stream.py` — SSE endpoint with version-based change detection
- [x] `__init__.py` — public API surface
- [x] 73 unit/integration tests, 84 % coverage
- [x] `market_data_demo.py` — Rich terminal dashboard

Remaining, in dependency order:

- [ ] **Wire into `app/main.py`** — `lifespan` creating the cache and source, registering
      the SSE router, `load_tracked_tickers()` from SQLite, `stop()` on shutdown (§8)
- [ ] **`get_price_cache` / `get_market_source` dependencies** for the trade, portfolio,
      and watchlist routes (§8)
- [ ] **Watchlist ↔ source reconciliation** — `reconcile_ticker()` called after watchlist
      mutations and position-opening/closing trades (§9)
- [ ] **Session-change baseline** — `PriceUpdate.baseline`, `PriceCache.set_baseline()`,
      wiring in both sources, so the watchlist can render a real day-change % (§10)
- [ ] **SSE integration test** — closes the 31 % coverage gap on `stream.py`; move the
      `APIRouter` construction inside the factory first (§7, §11.6)
- [ ] **Optional `MASSIVE_POLL_INTERVAL` env var** for paid tiers (§13)
