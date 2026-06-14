# src/cli/main.py

from pathlib import Path
from typing import Annotated, Optional
from config import setup_logging, PROJECT_ROOT
import typer, logging
from ingestion import MarketMetadataLoader

setup_logging()
logger = logging.getLogger(__name__)

app = typer.Typer(no_args_is_help=True)

@app.command("process-markets")
def process_markets():
    logger.info(f"Processing NBA game winner markets...")

@app.command("load-markets")
def load_markets(series_ticker: Annotated[Optional[str], typer.Option("--series-ticker", help="The NBA series ticker to load markets for")] = None, workers: Annotated[int, typer.Option("--workers", help="The number of workers to use for loading markets")] = 8):
    logger.info(f"Loading NBA game winner markets...")
    market_metadata_loader = MarketMetadataLoader(workers)
    market_metadata_loader.load_market_metadata(series_ticker)

if __name__ == "__main__":
    app()