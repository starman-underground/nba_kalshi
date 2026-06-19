import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import polars as pl
import requests

from config import RAW_MARKET_METADATA_LIBRARY
from db import ArcticStore

logger = logging.getLogger(__name__)


class MarketMetadataLoader:
    BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
    PAGE_LIMIT = 1000
    NBA_RE = re.compile(r"\bNBA\b")
    DEFAULT_MAX_WORKERS = 8

    def __init__(self, max_workers: int = DEFAULT_MAX_WORKERS, store: Optional[ArcticStore] = None):
        self.max_workers = max_workers
        self.store = store or ArcticStore()
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
        logger.info(f"Fetched {len(live)} live and {len(historical)} historical markets for {series_ticker}")
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

    def _pull_nba_markets(self, series_tickers: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """Return {series_ticker: [market_dicts]} with live preferred over historical."""
        logger.info(f"Fetching markets for {len(series_tickers)} NBA series")

        markets_by_series: Dict[str, List[Dict[str, Any]]] = {}
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
                by_ticker: Dict[str, dict] = {}
                self._merge_markets(by_ticker, live, historical)
                markets_by_series[series_ticker] = list(by_ticker.values())

        total = sum(len(v) for v in markets_by_series.values())
        logger.info(f"Total unique NBA markets: {total}")
        return markets_by_series

    def _pull_all_nba_markets(self) -> Dict[str, List[Dict[str, Any]]]:
        nba_series = self._discover_nba_series()
        logger.info(f"Found {len(nba_series)} NBA series")
        return self._pull_nba_markets(nba_series)

    def _write_markets_to_store(self, markets_by_series: Dict[str, List[Dict[str, Any]]]) -> None:
        if not markets_by_series:
            logger.warning("No markets to write")
            return

        for series_ticker, markets in sorted(markets_by_series.items()):
            if not markets:
                continue
            series_df = pl.DataFrame(markets)
            self.store.write(
                RAW_MARKET_METADATA_LIBRARY,
                series_ticker,
                series_df,
                index_col="ticker",
            )
            logger.info(f"Wrote {len(series_df)} markets to {series_ticker}")

    def read_market_metadata(
        self,
        series_ticker: Optional[str] = None,
    ) -> pl.DataFrame:
        """Read market metadata from ArcticDB, optionally filtered to one series."""
        if series_ticker:
            return self.store.read(RAW_MARKET_METADATA_LIBRARY, series_ticker)

        symbols = self.store.list_symbols(RAW_MARKET_METADATA_LIBRARY)
        if not symbols:
            return pl.DataFrame()

        frames = [
            self.store.read(RAW_MARKET_METADATA_LIBRARY, symbol).lazy()
            for symbol in symbols
        ]
        return pl.concat(frames).collect(streaming=True)

    def load_market_metadata(self, series_ticker: Optional[str] = None) -> None:
        """
        Load settled NBA market metadata from Kalshi into ArcticDB.

        When `series_ticker` is given, fetch only that series; otherwise discover
        and fetch all NBA series concurrently. Each series is stored as its own
        symbol in the market metadata library, indexed by market ticker.
        """
        if series_ticker:
            markets_by_series = self._pull_nba_markets([series_ticker])
        else:
            markets_by_series = self._pull_all_nba_markets()

        self._write_markets_to_store(markets_by_series)
