# Market Simulator Design

Approach and code structure for simulating realistic stock prices when `MASSIVE_API_KEY` is not set (the default — see `MARKET_INTERFACE.md` for how the simulator fits behind the shared `MarketDataSource` interface, and `MASSIVE_API.md` for the real-data alternative it stands in for).

This document describes the simulator as implemented in `backend/app/market/simulator.py` and `seed_prices.py`.

## Overview

The simulator uses **Geometric Brownian Motion (GBM)** to generate realistic stock price paths. GBM is the standard model underlying Black-Scholes option pricing — prices evolve continuously with random noise, can never go negative, and produce the lognormal-return distribution seen in real markets. On top of that it layers:

- **Correlated moves** across tickers, so sectors move together the way real markets do
- **Occasional shock events** — sudden 2-5% jumps — for visual drama on the dashboard

Updates run at ~500ms intervals (`SimulatorDataSource.update_interval`, see `MARKET_INTERFACE.md`), producing a continuous stream of small price changes that feels alive without requiring any external dependency.

## GBM Math

At each time step, a stock price evolves as:

```
S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)
```

Where:
- `S(t)` = current price
- `mu` = annualized drift (expected return), e.g. `0.05` (5%)
- `sigma` = annualized volatility, e.g. `0.20` (20%)
- `dt` = this time step, expressed as a fraction of a trading year
- `Z` = a (correlated) standard normal random variable

The `- sigma^2/2` term is the standard Itô correction — without it, the *expected* price would drift upward faster than `mu` implies, because compounding a lognormal variable is not the same as compounding its mean.

### Choosing `dt`

For 500ms ticks over a trading year of 252 days × 6.5 hours/day:

```python
TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR   # ~8.48e-8
```

This tiny `dt` produces small, sub-cent-scale moves per tick that accumulate naturally into realistic-looking intraday ranges over the course of a session — a stock with `sigma=0.50` (e.g. TSLA) should show roughly the volatility you'd expect from a real high-beta stock over a real trading day, just compressed into however long the demo runs.

## Correlated Moves

Real stocks don't move independently — tech names tend to move together, financials move together, etc. The simulator captures this with a **Cholesky decomposition** of a pairwise correlation matrix.

Given a correlation matrix `C` (symmetric, positive semi-definite), compute `L = cholesky(C)` such that `L @ L.T == C`. Then, given independent standard normal draws `Z_independent`:

```
Z_correlated = L @ Z_independent
```

`Z_correlated` has exactly the target covariance structure — this is the standard technique for generating correlated Gaussian samples from independent ones.

### Correlation structure

```python
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

INTRA_TECH_CORR = 0.6      # tech stocks move together
INTRA_FINANCE_CORR = 0.5   # finance stocks move together
CROSS_GROUP_CORR = 0.3     # different sectors, or an unrecognized ticker
TSLA_CORR = 0.3            # TSLA is checked first and always gets this, even vs. other tech
```

Pairwise lookup (`_pairwise_correlation`) checks `TSLA` first — so it always correlates at 0.3 with everything, including other tech names, keeping it as a stock that "does its own thing" rather than tracking the broader tech basket.

## Random Events

Each simulation step, every ticker independently has a small chance (`event_probability`, default `0.001` = 0.1%) of a sudden 2-5% move, applied on top of (not instead of) the normal GBM step:

```python
if random.random() < self._event_prob:
    shock_magnitude = random.uniform(0.02, 0.05)
    shock_sign = random.choice([-1, 1])
    self._prices[ticker] *= 1 + shock_magnitude * shock_sign
```

At 2 ticks/sec, a 0.1% per-tick probability means an event on a *given* ticker roughly every ~500 seconds (~8 minutes). Across a 10-ticker watchlist, that's an event *somewhere* roughly every ~50 seconds — frequent enough to keep the dashboard visually interesting without every ticker being in constant upheaval.

## Seed Prices & Per-Ticker Parameters

Realistic starting prices and volatility/drift for the default watchlist:

```python
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00, "GOOGL": 175.00, "MSFT": 420.00, "AMZN": 185.00,
    "TSLA": 250.00, "NVDA": 800.00, "META": 500.00, "JPM": 195.00,
    "V": 280.00, "NFLX": 600.00,
}

TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL":  {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT":  {"sigma": 0.20, "mu": 0.05},
    "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA":  {"sigma": 0.50, "mu": 0.03},   # high volatility
    "NVDA":  {"sigma": 0.40, "mu": 0.08},   # high volatility, strong drift
    "META":  {"sigma": 0.30, "mu": 0.05},
    "JPM":   {"sigma": 0.18, "mu": 0.04},   # low volatility (bank)
    "V":     {"sigma": 0.17, "mu": 0.04},   # low volatility (payments)
    "NFLX":  {"sigma": 0.35, "mu": 0.05},
}

# Applied to any ticker not in the table above — e.g. one the user or the LLM adds dynamically
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}
```

Per `PLAN.md` §6, a ticker added later that isn't in `SEED_PRICES`/`TICKER_PARAMS` gets a random seed price between $50-$300 and the generic `DEFAULT_PARAMS` — moderate, unremarkable volatility/drift rather than a per-ticker calibrated profile. The API/chat layer is responsible for flagging to the user that such a ticker's simulation is generic, not calibrated (the simulator itself has no concept of "flagging" — it just applies defaults silently).

## Implementation

```python
import math
import random
import numpy as np

class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices."""

    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR

    def __init__(self, tickers: list[str], dt: float = DEFAULT_DT, event_probability: float = 0.001) -> None:
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Hot path — called every 500ms."""
        n = len(self._tickers)
        if n == 0:
            return {}

        z_independent = np.random.standard_normal(n)
        z = self._cholesky @ z_independent if self._cholesky is not None else z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            mu, sigma = params["mu"], params["sigma"]

            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            if random.random() < self._event_prob:
                shock = random.uniform(0.02, 0.05) * random.choice([-1, 1])
                self._prices[ticker] *= (1 + shock)

            result[ticker] = round(self._prices[ticker], 2)
        return result

    def add_ticker(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add without rebuilding Cholesky — used for batch init, then rebuilt once."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """O(n^2) to build + O(n^3) to decompose; fine since n stays well under 50 tickers."""
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

### Why batch-init defers the Cholesky rebuild

`__init__` calls `_add_ticker_internal` (no rebuild) for every starting ticker, then rebuilds the correlation matrix **once** at the end — rebuilding after each of the 10 default tickers during startup would do 10x the work for no benefit, since only the final matrix (with all 10 present) matters. `add_ticker()`/`remove_ticker()` (called one at a time, post-startup) each trigger their own rebuild, since those changes need to take effect immediately.

## `SimulatorDataSource` — Wrapping in the `MarketDataSource` Interface

`GBMSimulator` itself is a pure, synchronous price-generation engine — it knows nothing about asyncio, caches, or the `MarketDataSource` contract. `SimulatorDataSource` (in the same `simulator.py` module) is the thin async wrapper that owns a `GBMSimulator` instance, runs it on a timer, and writes results into the shared `PriceCache`. See `MARKET_INTERFACE.md` for that wrapper's implementation and its place in the overall architecture — this document is scoped to the simulation engine itself.

## File Structure

```
backend/
  app/
    market/
      simulator.py       # GBMSimulator (pure simulation engine) + SimulatorDataSource (async wrapper)
      seed_prices.py      # SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS, CORRELATION_GROUPS, correlation constants
```

`seed_prices.py` holds only constant data (no logic) so it can be tuned — different seed prices, different volatility profiles, different correlation groupings — without touching the simulation code in `simulator.py`.

## Behavior Notes

- **Prices never go negative.** GBM is multiplicative (`exp(...)` is always positive), so this is a structural guarantee, not something that needs a clamp.
- **The tiny `dt` produces sub-cent moves per tick**, which compound naturally into realistic intraday ranges over time — this is why `dt` is derived from the real trading-year second count rather than picked arbitrarily.
- **The correlation matrix must stay positive semi-definite** for `np.linalg.cholesky` to succeed. The 3-tier structure used here (intra-tech 0.6, intra-finance 0.5, everything else 0.3, with TSLA pinned to 0.3 against everyone) is simple enough to guarantee this by construction — an arbitrary hand-edited correlation matrix would not have that guarantee and should be validated before use.
- **Adding/removing a ticker mid-session rebuilds the Cholesky matrix.** `O(n^2)` to build the correlation matrix, `O(n^3)` for the decomposition itself — negligible for the tens-of-tickers scale this watchlist operates at, but would need revisiting if the ticker count ever grew into the hundreds.
- **Rounding to `round(price, 2)`** happens in `step()`'s output, not in the internal `_prices` state — internal state stays at full float precision so tiny per-tick drifts don't get lost to rounding across thousands of ticks; only the externally-visible result is cent-rounded.
