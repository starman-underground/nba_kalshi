
# Overview

A Python-based calibration and backtesting system that uses Merton jump-diffusion modeling to identify mispricings in Kalshi NBA event contracts.

# Data Sources
 - Kalshi market metadata
     - split between live and historical endpoints
        - live: https://external-api.kalshi.com/trade-api/v2/markets
        - historical: https://external-api.kalshi.com/trade-api/v2/historical/markets

# Architecture

```
nba-kalshi/
├── data/
│   ├── raw/                      # Kalshi trades + market metadata cache (parquet)
│   ├── filtered/                 # Latent x̂ series (A1)
│   ├── calibrated/               # EM params, surfaces, models (A2/A4/C2)
│   └── aligned/                  # Joined datasets (C1)
├── src/
│   ├── __init__.py
│   ├── cli.py
│   ├── ingestion/
│   │   ├── kalshi_market.py
│   │   ├── nba_pbp.py            # NBA CDN endpoint
│   │   ├── game_state.py
│   │   └── alignment.py          # convergence only
│   ├── calibration/
│   │   ├── filtering.py          # heteroskedastic Kalman
│   │   ├── em.py                 # diffusion/jump EM + RN drift
│   │   ├── forecast_eval.py      # variance-forecast gate
│   │   └── surface.py            # (τ, m) belief-vol surface
│   ├── pricing/
│   │   ├── kernel.py             # logit JD: S(x), drift, simulation
│   │   ├── win_prob.py
│   │   └── signals.py            # directional signal
│   ├── backtesting/
│   │   ├── simulator.py
│   │   ├── execution.py
│   │   └── metrics.py
│   └── utils/
│       ├── constants.py
│       └── time_utils.py
├── notebooks/
│   ├── 01_kalshi_market_data.ipynb
│   └── 02_find_best_markets.ipynb
├── tests/
├── pyproject.toml
└── README.md
```