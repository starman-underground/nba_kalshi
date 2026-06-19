import logging
from datetime import datetime
from typing import Callable, Dict, Optional

import polars as pl
from pydantic import ValidationError

from config import (
    PREPROCESSED_MARKET_METADATA_LIBRARY,
    RAW_MARKET_METADATA_LIBRARY,
    TICKER_TO_TEAM_MAP,
)
from db import ArcticStore
from models.game_market_metadata import GameMarketMetadata

logger = logging.getLogger(__name__)

SeriesPreprocessor = Callable[[pl.DataFrame], pl.DataFrame]


def _parse_iso_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _team_name(team_code: str) -> str:
    try:
        return TICKER_TO_TEAM_MAP[team_code]
    except KeyError as exc:
        raise ValueError(f"Unknown team code {team_code!r}") from exc


def _parse_kxnbagame_teams(ticker: str) -> tuple[list[str], str, str]:
    team_codes = (ticker[-10:-7], ticker[-7:-4], ticker[-3:])
    teams = [_team_name(team_codes[0]), _team_name(team_codes[1])]
    yes_winner = _team_name(team_codes[2])
    no_winner = teams[0] if yes_winner == teams[1] else teams[1]
    return teams, yes_winner, no_winner


def _parse_kxnbagame_row(row: dict) -> Optional[GameMarketMetadata]:
    if row["result"] == "scalar":
        logger.debug(
            "%s: scalar result, game was postponed/canceled",
            row["ticker"],
        )
        return None

    teams, yes_winner, no_winner = _parse_kxnbagame_teams(row["ticker"])

    return GameMarketMetadata(
        ticker=row["ticker"],
        title=row["title"],
        rules_primary=row["rules_primary"],
        event_ticker=row["event_ticker"],
        result=row["result"],
        yes_winner=yes_winner,
        no_winner=no_winner,
        volume=float(row["volume_fp"]),
        open_ts=_parse_iso_datetime(row["open_time"]),
        settlement_ts=_parse_iso_datetime(row["settlement_ts"]),
        expected_expiration_time=_parse_iso_datetime(row["expected_expiration_time"]),
        expiration_ts=_parse_iso_datetime(row["expiration_time"]),
        teams=teams,
    )


class MarketMetadataPreprocessor:
    KXNBAGAME_EVENT_TICKER = "KXNBAGAME"

    def __init__(self, store: Optional[ArcticStore] = None):
        self.store = store or ArcticStore()
        self._series_preprocessors: Dict[str, SeriesPreprocessor] = {
            self.KXNBAGAME_EVENT_TICKER: self._preprocess_kxnbagame,
        }

    def register_series_preprocessor(
        self,
        series_ticker: str,
        preprocessor: SeriesPreprocessor,
    ) -> None:
        """Register a preprocessor for a future event/series ticker."""
        self._series_preprocessors[series_ticker] = preprocessor

    def _get_series_preprocessor(self, series_ticker: str) -> SeriesPreprocessor:
        try:
            return self._series_preprocessors[series_ticker]
        except KeyError as exc:
            raise NotImplementedError(
                f"Preprocessing for series {series_ticker!r} is not implemented"
            ) from exc

    def _preprocess_kxnbagame(self, market_metadata: pl.DataFrame) -> pl.DataFrame:
        records: list[dict] = []
        skipped_scalar = 0
        validation_errors = 0

        for row in market_metadata.iter_rows(named=True):
            try:
                parsed = _parse_kxnbagame_row(row)
            except (KeyError, TypeError, ValueError, ValidationError) as exc:
                validation_errors += 1
                logger.warning(
                    "Skipping %s due to parse error: %s",
                    row.get("ticker"),
                    exc,
                )
                continue

            if parsed is None:
                skipped_scalar += 1
                continue

            records.append(parsed.model_dump())

        if skipped_scalar:
            logger.info(
                "Skipped %d KXNBAGAME markets with scalar results",
                skipped_scalar,
            )
        if validation_errors:
            logger.warning(
                "Skipped %d KXNBAGAME markets due to validation errors",
                validation_errors,
            )

        if not records:
            return pl.DataFrame()

        return pl.DataFrame(records)

    def preprocess_market_metadata(
        self,
        market_metadata: pl.DataFrame,
        series_ticker: str,
    ) -> pl.DataFrame:
        preprocessor = self._get_series_preprocessor(series_ticker)
        return preprocessor(market_metadata)

    def preprocess_and_store(self, series_ticker: str) -> pl.DataFrame:
        raw = self.store.read(RAW_MARKET_METADATA_LIBRARY, series_ticker)
        preprocessed = self.preprocess_market_metadata(raw, series_ticker)

        if preprocessed.is_empty():
            logger.warning("No preprocessed markets to write for %s", series_ticker)
            return preprocessed

        self.store.write(
            PREPROCESSED_MARKET_METADATA_LIBRARY,
            series_ticker,
            preprocessed,
            index_col="ticker",
        )
        logger.info(
            "Wrote %d preprocessed markets to %s",
            len(preprocessed),
            series_ticker,
        )
        return preprocessed

    def read_preprocessed_market_metadata(
        self,
        series_ticker: Optional[str] = None,
    ) -> pl.DataFrame:
        if series_ticker:
            return self.store.read(
                PREPROCESSED_MARKET_METADATA_LIBRARY,
                series_ticker,
            )

        symbols = self.store.list_symbols(PREPROCESSED_MARKET_METADATA_LIBRARY)
        if not symbols:
            return pl.DataFrame()

        frames = [
            self.store.read(PREPROCESSED_MARKET_METADATA_LIBRARY, symbol).lazy()
            for symbol in symbols
        ]
        return pl.concat(frames).collect(streaming=True)
