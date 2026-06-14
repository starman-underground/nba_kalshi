# src/cli/main.py
from pathlib import Path
from typing import Annotated
import typer, logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)",
    datefmt="%Y-%m-%d %H:%M:%S %z"
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
app = typer.Typer(no_args_is_help=True)

@app.command("process-markets")
def process_markets():
    logger.info(f"Processing NBA game winner markets...")

@app.command("load-markets")
def load_markets():
    logger.info(f"Loading NBA game winner markets...")
    data_dir = PROJECT_ROOT / "data" / "raw"
    data_dir.mkdir(parents=True, exist_ok=True)

if __name__ == "__main__":
    app()