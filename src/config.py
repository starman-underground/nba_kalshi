import logging.config
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATABASE_URI = f"lmdb:///{PROJECT_ROOT / 'data'}"
MARKET_METADATA_LIBRARY = "market_metadata"

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(asctime)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)",
            "datefmt": "%Y-%m-%d %H:%M:%S %z"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "default",
            "stream": "ext://sys.stdout"
        }
    },
    "root": {
        "handlers": ["console"],
        "level": "INFO"
    }
}

def setup_logging():
    logging.config.dictConfig(LOGGING_CONFIG)