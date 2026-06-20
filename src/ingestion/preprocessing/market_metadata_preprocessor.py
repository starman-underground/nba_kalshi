import logging
from datetime import date, datetime
from typing import Callable, Dict, Optional
from zoneinfo import ZoneInfo

import polars as pl
from pydantic import ValidationError
from sportsdataverse.nba import load_nba_schedule

from config import (
    MONTHS,
    PREPROCESSED_MARKET_METADATA_LIBRARY,
    RAW_MARKET_METADATA_LIBRARY,
    TICKER_TO_TEAM_MAP,
    arena_timezone,
)
from db import ArcticStore
from models.game_market_metadata import GameMarketMetadata

logger = logging.getLogger(__name__)

SeriesPreprocessor = Callable[[pl.DataFrame], pl.DataFrame]

_TIMESTAMP_UNIX_COLUMNS = {
    "open_ts": "open_ts_unix",
    "settlement_ts": "settlement_ts_unix",
    "expected_expiration_time": "expected_expiration_time_unix",
    "expiration_ts": "expiration_ts_unix",
    "tip_off_ts_utc": "tip_off_ts_unix",
}


def _add_unix_timestamp_columns(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df

    return df.with_columns(
        pl.col(ts_col).dt.epoch(time_unit="s").alias(unix_col)
        for ts_col, unix_col in _TIMESTAMP_UNIX_COLUMNS.items()
        if ts_col in df.columns
    )

def _parse_iso_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _team_name(team_code: str) -> str:
    try:
        return TICKER_TO_TEAM_MAP[team_code]
    except KeyError as exc:
        raise ValueError(f"Unknown team code {team_code!r}") from exc


def _parse_kxnbagame_event_ticker(event_ticker: str) -> tuple[date, frozenset[str]]:
    suffix = event_ticker.split("-", 1)[1]
    if len(suffix) < 13:
        raise ValueError(f"Invalid KXNBAGAME event ticker {event_ticker!r}")

    date_token = suffix[:7]
    away_code, home_code = suffix[7:10], suffix[10:13]
    year = 2000 + int(date_token[:2])
    month = MONTHS[date_token[2:5]]
    day = int(date_token[5:7])
    teams = frozenset(
        {_team_name(away_code), _team_name(home_code)},
    )
    return date(year, month, day), teams


def _nba_season_for_date(game_date: date) -> int:
    return game_date.year + 1 if game_date.month >= 10 else game_date.year


def _to_local_tip_off(
    tip_off_utc: datetime,
    game_tz: Optional[str],
) -> Optional[datetime]:
    if game_tz is None:
        return None
    return tip_off_utc.astimezone(ZoneInfo(game_tz)).replace(tzinfo=None)


def _build_game_schedule_lookup(
    game_dates: set[date],
) -> dict[tuple[date, frozenset[str]], tuple[datetime, Optional[str], str]]:
    if not game_dates:
        return {}

    seasons = sorted({_nba_season_for_date(game_date) for game_date in game_dates})
    schedule = load_nba_schedule(seasons=seasons)
    lookup: dict[tuple[date, frozenset[str]], tuple[datetime, Optional[str], str]] = {}

    for row in schedule.iter_rows(named=True):
        teams = frozenset({row["home_display_name"], row["away_display_name"]})
        key = (row["game_date"], teams)
        tip_off_ts = _parse_iso_datetime(row["date"])
        arena = row["venue_full_name"]
        game_tz = arena_timezone(arena)
        if game_tz is None:
            logger.warning(
                "No timezone mapping for ESPN venue %r",
                arena,
            )
        lookup[key] = (tip_off_ts, game_tz, arena)

    return lookup


def _enrich_with_schedule(
    market_metadata: pl.DataFrame,
    schedule_lookup: dict[tuple[date, frozenset[str]], tuple[datetime, Optional[str], str]],
) -> pl.DataFrame:
    tip_off_utc_values: list[Optional[datetime]] = []
    tip_off_local_values: list[Optional[datetime]] = []
    game_tz_values: list[Optional[str]] = []
    arena_values: list[Optional[str]] = []
    missing_events: list[str] = []

    for row in market_metadata.iter_rows(named=True):
        try:
            game_date, teams = _parse_kxnbagame_event_ticker(row["event_ticker"])
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning(
                "Could not parse event ticker %s for schedule enrichment: %s",
                row.get("event_ticker"),
                exc,
            )
            tip_off_utc_values.append(None)
            tip_off_local_values.append(None)
            game_tz_values.append(None)
            arena_values.append(None)
            continue

        match = schedule_lookup.get((game_date, teams))
        if match is None:
            missing_events.append(row["event_ticker"])
            tip_off_utc_values.append(None)
            tip_off_local_values.append(None)
            game_tz_values.append(None)
            arena_values.append(None)
            continue

        tip_off_utc, game_tz, arena = match
        tip_off_utc_values.append(tip_off_utc)
        tip_off_local_values.append(_to_local_tip_off(tip_off_utc, game_tz))
        game_tz_values.append(game_tz)
        arena_values.append(arena)

    if missing_events:
        unique_missing = sorted(set(missing_events))
        logger.warning(
            "No ESPN schedule match for %d KXNBAGAME events (showing up to 5): %s",
            len(unique_missing),
            ", ".join(unique_missing[:5]),
        )

    return market_metadata.with_columns(
        pl.Series("tip_off_ts_utc", tip_off_utc_values, dtype=pl.Datetime("us", "UTC")),
        pl.Series("tip_off_ts_local", tip_off_local_values, dtype=pl.Datetime("us")),
        pl.Series("game_tz", game_tz_values),
        pl.Series("arena", arena_values),
    )


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

        preprocessed = pl.DataFrame(records)
        game_dates: set[date] = set()
        for event_ticker in preprocessed["event_ticker"].unique().to_list():
            try:
                game_date, _ = _parse_kxnbagame_event_ticker(event_ticker)
            except (KeyError, TypeError, ValueError) as exc:
                logger.warning(
                    "Could not parse event ticker %s while loading ESPN schedule: %s",
                    event_ticker,
                    exc,
                )
                continue
            game_dates.add(game_date)
        schedule_lookup = _build_game_schedule_lookup(game_dates)
        return _enrich_with_schedule(preprocessed, schedule_lookup)

    def preprocess_market_metadata(
        self,
        market_metadata: pl.DataFrame,
        series_ticker: str,
    ) -> pl.DataFrame:
        preprocessor = self._get_series_preprocessor(series_ticker)
        return _add_unix_timestamp_columns(preprocessor(market_metadata))

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
