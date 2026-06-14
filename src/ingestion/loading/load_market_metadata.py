import json
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

from config import PROJECT_ROOT

logger = logging.getLogger(__name__)


class MarketMetadataLoader:
    BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
    PAGE_LIMIT = 1000
    NBA_RE = re.compile(r"\bNBA\b")
    DEFAULT_MAX_WORKERS = 8

    def __init__(self, max_workers: int = DEFAULT_MAX_WORKERS):
        self.metadata_dir = PROJECT_ROOT / "data" / "raw" / "market_metadata"
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        self.max_workers = max_workers
        self._local = threading.local()

    @property
    def session(self) -> requests.Session:
        """One session per worker thread; requests.Session is not thread-safe."""
        if not hasattr(self._local, "session"):
            session = requests.Session()
            session.headers.update({"Accept": "application/json"})
            self._local.session = session
        return self._local.session

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        """GET with simple exponential backoff on 429 / 5xx."""
        url = f"{self.BASE_URL}{path}"
        delay = 1.0
        resp = None
        for _ in range(6):
            resp = self.session.get(url, params=params, timeout=30)
            if resp.status_code == 429 or resp.status_code >= 500:
                time.sleep(delay)
                delay = min(delay * 2, 30)
                continue
            resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()
        raise RuntimeError("unreachable")

    def _paginate(self, path: str, params: Optional[dict], list_key: str):
        """
        Yield every item from a cursor-paginated Kalshi list endpoint.

        `list_key` is the field holding the array, e.g. "markets" or "series".
        Kalshi returns a top-level "cursor"; an empty/absent cursor means done.
        """
        params = dict(params or {})
        params.setdefault("limit", self.PAGE_LIMIT)
        while True:
            data = self._get(path, params)
            for item in data.get(list_key, []) or []:
                yield item
            cursor = data.get("cursor")
            if not cursor:
                return
            params["cursor"] = cursor

    def _discover_nba_series(self) -> List[str]:
        """Return the list of series tickers whose title/tags reference the NBA."""
        tickers = []
        for series in self._paginate("/series", {"category": "Sports"}, "series"):
            title = series.get("title", "") or ""
            tags = " ".join(series.get("tags", []) or [])
            ticker = series.get("ticker", "") or ""
            if self.NBA_RE.search(title) or self.NBA_RE.search(tags) or ticker.upper().startswith("KXNBA"):
                tickers.append(ticker)
        return sorted(set(tickers))

    def _markets_for_series(self, series_ticker: str):
        """Settled markets for one series from the live API."""
        yield from self._paginate(
            "/markets",
            {"series_ticker": series_ticker, "status": "settled"},
            "markets",
        )

    def _historical_markets_for_series(self, series_ticker: str):
        """Markets for one series archived past the historical cutoff."""
        yield from self._paginate(
            "/historical/markets",
            {"series_ticker": series_ticker},
            "markets",
        )

    def _fetch_series_markets(self, series_ticker: str) -> Tuple[List[dict], List[dict]]:
        live = list(self._markets_for_series(series_ticker))
        historical = list(self._historical_markets_for_series(series_ticker))
        return live, historical

    def _merge_markets(
        self,
        by_ticker: Dict[str, dict],
        live: List[dict],
        historical: List[dict],
    ) -> None:
        for market in live:
            by_ticker[market["ticker"]] = market
        for market in historical:
            by_ticker.setdefault(market["ticker"], market)

    def _pull_nba_markets(self, series_tickers: List[str]) -> List[Dict[str, Any]]:
        logger.info(f"Fetching markets for {len(series_tickers)} NBA series")

        by_ticker: Dict[str, dict] = {}
        workers = min(self.max_workers, max(1, len(series_tickers)))

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(self._fetch_series_markets, ticker): ticker
                for ticker in series_tickers
            }
            for future in as_completed(futures):
                series_ticker = futures[future]
                try:
                    live, historical = future.result()
                except Exception:
                    logger.exception(f"Failed fetching markets for series {series_ticker}")
                    raise
                self._merge_markets(by_ticker, live, historical)
                logger.debug(
                    f"{series_ticker}: {len(live)} live, {len(historical)} historical"
                )

        markets = list(by_ticker.values())
        logger.info(f"Total unique NBA markets: {len(markets)}")
        return markets

    def _pull_all_nba_markets(self) -> List[Dict[str, Any]]:
        nba_series = self._discover_nba_series()
        logger.info(f"Found {len(nba_series)} NBA series")
        return self._pull_nba_markets(nba_series)

    def load_market_metadata(self, series_ticker: Optional[str] = None) -> pd.DataFrame:
        """
        Load settled NBA market metadata.

        When `series_ticker` is given, fetch only that series; otherwise discover
        and fetch all NBA series concurrently.
        """
        if series_ticker:
            markets = self._pull_nba_markets([series_ticker])
            output_path = self.metadata_dir / f"{series_ticker}.json"
        else:
            markets = self._pull_all_nba_markets()
            output_path = self.metadata_dir / "nba_markets.json"

        with output_path.open("w") as f:
            json.dump(markets, f, indent=4)
        logger.info(f"Wrote {len(markets)} markets to {output_path}")

        return pd.DataFrame(markets)
