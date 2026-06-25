import logging
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import polars as pl
import requests

from config import PREPROCESSED_MARKET_METADATA_LIBRARY, pre_tip_candles_library
from db import ArcticStore

logger = logging.getLogger(__name__)


def _parse_iso_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=ZoneInfo("UTC"))
    return value.astimezone(ZoneInfo("UTC"))


class PreTipCandlestickLoader:
    BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
    PERIOD_INTERVAL_MINUTES = 1
    MAX_CANDLES_PER_REQUEST = 9_999
    DEFAULT_MAX_WORKERS = 4
    KXNBAGAME_SERIES_TICKER = "KXNBAGAME"

    _PRICE_FIELDS = ("open", "high", "low", "close", "mean", "previous", "min", "max")
    _SIDE_FIELDS = ("open", "high", "low", "close")

    def __init__(
        self,
        max_workers: int = DEFAULT_MAX_WORKERS,
        store: Optional[ArcticStore] = None,
    ):
        self.max_workers = max_workers
        self.store = store or ArcticStore()
        self._local = threading.local()
        self._market_settled_cutoff: Optional[datetime] = None

    @property
    def session(self) -> requests.Session:
        if not hasattr(self._local, "session"):
            session = requests.Session()
            session.headers.update({"Accept": "application/json"})
            self._local.session = session
        return self._local.session

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        url = f"{self.BASE_URL}{path}"
        delay = 1.0
        resp = None
        for _ in range(6):
            resp = self.session.get(url, params=params, timeout=60)
            if resp.status_code == 429 or resp.status_code >= 500:
                time.sleep(delay)
                delay = min(delay * 2, 30)
                continue
            resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()
        raise RuntimeError("unreachable")

    def fetch_market_settled_cutoff(self) -> datetime:
        """Return Kalshi's live/historical partition for settled markets."""
        if self._market_settled_cutoff is None:
            data = self._get("/historical/cutoff")
            self._market_settled_cutoff = _parse_iso_datetime(data["market_settled_ts"])
            logger.info(
                "Kalshi market_settled_ts cutoff: %s",
                self._market_settled_cutoff.isoformat(),
            )
        return self._market_settled_cutoff

    def market_uses_historical_candlesticks(self, settlement_ts: datetime) -> bool:
        return _ensure_utc(settlement_ts) < self.fetch_market_settled_cutoff()

    @classmethod
    def _candlestick_schema(cls) -> dict[str, pl.DataType]:
        schema: dict[str, pl.DataType] = {
            "end_period_ts": pl.Int64,
            "volume_fp": pl.Utf8,
            "open_interest_fp": pl.Utf8,
        }
        for side in ("yes_bid", "yes_ask"):
            for field in cls._SIDE_FIELDS:
                schema[f"{side}_{field}_dollars"] = pl.Utf8
        for field in cls._PRICE_FIELDS:
            schema[f"price_{field}_dollars"] = pl.Utf8
        return schema

    @classmethod
    def _dollar_value(cls, distribution: dict, field: str) -> Optional[str]:
        if not distribution:
            return None
        value = distribution.get(f"{field}_dollars")
        if value is None:
            value = distribution.get(field)
        return None if value is None else str(value)

    @classmethod
    def _flatten_candlestick(cls, candle: dict) -> dict:
        row = {
            "end_period_ts": candle["end_period_ts"],
            "volume_fp": candle.get("volume_fp") or candle.get("volume"),
            "open_interest_fp": candle.get("open_interest_fp") or candle.get("open_interest"),
        }
        for side in ("yes_bid", "yes_ask"):
            distribution = candle.get(side) or {}
            for field in cls._SIDE_FIELDS:
                row[f"{side}_{field}_dollars"] = cls._dollar_value(distribution, field)
        price = candle.get("price") or {}
        for field in cls._PRICE_FIELDS:
            row[f"price_{field}_dollars"] = cls._dollar_value(price, field)
        return row

    @classmethod
    def _candlesticks_to_frame(cls, candlesticks: list[dict]) -> pl.DataFrame:
        if not candlesticks:
            return pl.DataFrame()
        return (
            pl.DataFrame(
                [cls._flatten_candlestick(candle) for candle in candlesticks],
                schema=cls._candlestick_schema(),
            )
            .unique(subset=["end_period_ts"], keep="last")
            .sort("end_period_ts")
        )

    def _fetch_live_candlesticks_batch(
        self,
        market_tickers: list[str],
        start_ts: int,
        end_ts: int,
        *,
        include_latest_before_start: bool,
    ) -> dict[str, list[dict]]:
        if not market_tickers:
            return {}

        params = {
            "market_tickers": ",".join(market_tickers),
            "start_ts": start_ts,
            "end_ts": end_ts,
            "period_interval": self.PERIOD_INTERVAL_MINUTES,
            "include_latest_before_start": str(include_latest_before_start).lower(),
        }
        data = self._get("/markets/candlesticks", params)
        return {
            market["market_ticker"]: market.get("candlesticks") or []
            for market in data.get("markets") or []
        }

    def _fetch_historical_candlesticks(
        self,
        market_ticker: str,
        start_ts: int,
        end_ts: int,
    ) -> list[dict]:
        params = {
            "start_ts": start_ts,
            "end_ts": end_ts,
            "period_interval": self.PERIOD_INTERVAL_MINUTES,
        }
        data = self._get(f"/historical/markets/{market_ticker}/candlesticks", params)
        return data.get("candlesticks") or []

    def _iter_time_chunks(self, start_ts: int, end_ts: int):
        chunk_seconds = self.MAX_CANDLES_PER_REQUEST * self.PERIOD_INTERVAL_MINUTES * 60
        chunk_start = start_ts
        while chunk_start < end_ts:
            chunk_end = min(chunk_start + chunk_seconds, end_ts)
            yield chunk_start, chunk_end
            chunk_start = chunk_end

    def fetch_market_pre_tip_candlesticks(
        self,
        market_ticker: str,
        open_ts_unix: int,
        tip_off_ts_unix: int,
        *,
        use_historical: bool = False,
    ) -> pl.DataFrame:
        frames: list[pl.DataFrame] = []
        for index, (chunk_start, chunk_end) in enumerate(
            self._iter_time_chunks(open_ts_unix, tip_off_ts_unix)
        ):
            if use_historical:
                candlesticks = self._fetch_historical_candlesticks(
                    market_ticker,
                    chunk_start,
                    chunk_end,
                )
            else:
                by_ticker = self._fetch_live_candlesticks_batch(
                    [market_ticker],
                    chunk_start,
                    chunk_end,
                    include_latest_before_start=index == 0,
                )
                candlesticks = by_ticker.get(market_ticker, [])

            chunk = self._candlesticks_to_frame(candlesticks)
            if not chunk.is_empty():
                frames.append(chunk)

        if not frames:
            return pl.DataFrame()

        return (
            pl.concat(frames)
            .unique(subset=["end_period_ts"], keep="last")
            .sort("end_period_ts")
        )

    @classmethod
    def _valid_pre_tip_window(
        cls,
        open_ts_unix: float,
        tip_off_ts_unix: float,
    ) -> bool:
        if math.isnan(open_ts_unix) or math.isnan(tip_off_ts_unix):
            return False
        open_ts = int(open_ts_unix)
        tip_off_ts = int(tip_off_ts_unix)
        return tip_off_ts > open_ts

    def _read_preprocessed_markets(self, series_ticker: str) -> pl.DataFrame:
        return self.store.read(PREPROCESSED_MARKET_METADATA_LIBRARY, series_ticker)

    def read_pre_tip_candles(
        self,
        market_ticker: str,
        series_ticker: str = KXNBAGAME_SERIES_TICKER,
    ) -> pl.DataFrame:
        """Read pre-tip candlesticks for one market from ArcticDB."""
        return self.store.read(pre_tip_candles_library(series_ticker), market_ticker)

    def _fetch_and_store_market(
        self,
        *,
        library_name: str,
        market_ticker: str,
        open_ts_unix: int,
        tip_off_ts_unix: int,
        use_historical: bool,
        skip_existing: bool,
    ) -> tuple[str, int, Optional[str]]:
        if skip_existing and self.store.has_symbol(library_name, market_ticker):
            return market_ticker, 0, None

        candles = self.fetch_market_pre_tip_candlesticks(
            market_ticker,
            open_ts_unix,
            tip_off_ts_unix,
            use_historical=use_historical,
        )
        if candles.is_empty():
            return market_ticker, 0, "no candlesticks returned"

        self.store.write(
            library_name,
            market_ticker,
            candles,
            index_col="end_period_ts",
        )
        return market_ticker, len(candles), None

    def load_pre_tip_candles(
        self,
        series_ticker: str = KXNBAGAME_SERIES_TICKER,
        *,
        skip_existing: bool = True,
    ) -> dict[str, int]:
        """
        Load pre-tip minute candlesticks from Kalshi into ArcticDB.

        Each series is stored in its own library; each market is a symbol indexed
        by end_period_ts. Requires preprocessed market metadata with tip-off times.
        Markets settled before Kalshi's historical cutoff use the historical API.
        """
        cutoff = self.fetch_market_settled_cutoff()
        markets = self._read_preprocessed_markets(series_ticker)
        library_name = pre_tip_candles_library(series_ticker)

        eligible = markets.filter(
            pl.col("open_ts_unix").is_not_nan()
            & pl.col("tip_off_ts_unix").is_not_nan()
        )
        skipped_invalid = len(markets) - len(eligible)

        rows: list[dict] = []
        for row in eligible.iter_rows(named=True):
            if not self._valid_pre_tip_window(row["open_ts_unix"], row["tip_off_ts_unix"]):
                continue
            rows.append(
                {
                    **row,
                    "use_historical": _ensure_utc(row["settlement_ts"]) < cutoff,
                }
            )
        skipped_invalid += len(eligible) - len(rows)

        if skipped_invalid:
            logger.warning(
                "Skipped %d %s markets with invalid pre-tip windows",
                skipped_invalid,
                series_ticker,
            )

        if not rows:
            logger.warning("No eligible markets to load pre-tip candlesticks for %s", series_ticker)
            return {"fetched": 0, "skipped_invalid": skipped_invalid, "skipped_existing": 0, "empty": 0, "failed": 0}

        historical_count = sum(1 for row in rows if row["use_historical"])
        live_count = len(rows) - historical_count
        logger.info(
            "Loading %d %s markets (%d historical, %d live)",
            len(rows),
            series_ticker,
            historical_count,
            live_count,
        )

        stats = {
            "fetched": 0,
            "skipped_invalid": skipped_invalid,
            "skipped_existing": 0,
            "empty": 0,
            "failed": 0,
            "candles_written": 0,
        }
        workers = min(self.max_workers, max(1, len(rows)))

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    self._fetch_and_store_market,
                    library_name=library_name,
                    market_ticker=row["ticker"],
                    open_ts_unix=int(row["open_ts_unix"]),
                    tip_off_ts_unix=int(row["tip_off_ts_unix"]),
                    use_historical=row["use_historical"],
                    skip_existing=skip_existing,
                ): row["ticker"]
                for row in rows
            }
            for future in as_completed(futures):
                market_ticker = futures[future]
                try:
                    ticker, candle_count, error = future.result()
                except Exception:
                    stats["failed"] += 1
                    logger.exception("Failed loading pre-tip candlesticks for %s", market_ticker)
                    continue

                if error == "no candlesticks returned":
                    stats["empty"] += 1
                    logger.debug("%s: no candlesticks returned", ticker)
                elif candle_count == 0:
                    stats["skipped_existing"] += 1
                else:
                    stats["fetched"] += 1
                    stats["candles_written"] += candle_count
                    logger.debug("Wrote %d candlesticks for %s", candle_count, ticker)

        logger.info(
            "Pre-tip candlesticks for %s in %s: fetched=%d skipped_existing=%d empty=%d failed=%d candles=%d",
            series_ticker,
            library_name,
            stats["fetched"],
            stats["skipped_existing"],
            stats["empty"],
            stats["failed"],
            stats["candles_written"],
        )
        return stats
