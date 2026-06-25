# src/cli/main.py

from pathlib import Path
from typing import Annotated, Optional
from config import setup_logging, PROJECT_ROOT
import typer, logging
from ingestion import MarketMetadataLoader, MarketMetadataPreprocessor, PreTipCandlestickLoader

setup_logging()
logger = logging.getLogger(__name__)

app = typer.Typer(no_args_is_help=True)

@app.command("preprocess-markets")
def preprocess_markets(
    series_ticker: Annotated[
        str,
        typer.Option(
            "--series-ticker",
            help="The NBA series ticker to preprocess (e.g. KXNBAGAME)",
        ),
    ] = MarketMetadataPreprocessor.KXNBAGAME_EVENT_TICKER,
):
    logger.info("Preprocessing market metadata for %s...", series_ticker)
    MarketMetadataPreprocessor().preprocess_and_store(series_ticker)

@app.command("load-pre-tip-candles")
def load_pre_tip_candles(
    series_ticker: Annotated[
        str,
        typer.Option(
            "--series-ticker",
            help="The NBA series ticker to load pre-tip candlesticks for (e.g. KXNBAGAME)",
        ),
    ] = PreTipCandlestickLoader.KXNBAGAME_SERIES_TICKER,
    workers: Annotated[
        int,
        typer.Option(
            "--workers",
            help="Concurrent Kalshi API workers",
        ),
    ] = PreTipCandlestickLoader.DEFAULT_MAX_WORKERS,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Re-fetch markets even if candlesticks are already stored",
        ),
    ] = False,
):
    logger.info("Loading pre-tip minute candlesticks for %s...", series_ticker)
    PreTipCandlestickLoader(max_workers=workers).load_pre_tip_candles(
        series_ticker,
        skip_existing=not force,
    )


@app.command("load-markets")
def load_markets(
    series_ticker: Annotated[
        Optional[str],
        typer.Option(
            "--series-ticker",
            help="The NBA series ticker to load markets for",
        )
    ] = None,
    workers: Annotated[
        int,
        typer.Option(
            "--workers",
            help="The number of workers to use for loading markets",
        )
    ] = 8,
):
    logger.info(f"Loading NBA game winner markets...")
    market_metadata_loader = MarketMetadataLoader(workers)
    market_metadata_loader.load_market_metadata(series_ticker)

if __name__ == "__main__":
    app()