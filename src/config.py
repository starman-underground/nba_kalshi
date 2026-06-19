import logging.config
from pathlib import Path
from typing import Dict, Optional
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATABASE_URI = f"lmdb:///{PROJECT_ROOT / 'data'}"
RAW_MARKET_METADATA_LIBRARY = "raw_market_metadata"
PREPROCESSED_MARKET_METADATA_LIBRARY = "preprocessed_market_metadata"

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

TICKER_TO_TEAM_MAP: Dict[str, str] = {
    "ATL": "Atlanta Hawks",
    "BOS": "Boston Celtics",
    "BKN": "Brooklyn Nets",
    "CHA": "Charlotte Hornets",
    "CHI": "Chicago Bulls",
    "CLE": "Cleveland Cavaliers",
    "DET": "Detroit Pistons",
    "IND": "Indiana Pacers",
    "MIA": "Miami Heat",
    "MIL": "Milwaukee Bucks",
    "NYK": "New York Knicks",
    "ORL": "Orlando Magic",
    "PHI": "Philadelphia 76ers",
    "TOR": "Toronto Raptors",
    "WAS": "Washington Wizards",
    "DAL": "Dallas Mavericks",
    "DEN": "Denver Nuggets",
    "GSW": "Golden State Warriors",
    "HOU": "Houston Rockets",
    "LAC": "LA Clippers",
    "LAL": "Los Angeles Lakers",
    "MEM": "Memphis Grizzlies",
    "MIN": "Minnesota Timberwolves",
    "NOP": "New Orleans Pelicans",
    "OKC": "Oklahoma City Thunder",
    "PHX": "Phoenix Suns",
    "POR": "Portland Trail Blazers",
    "SAC": "Sacramento Kings",
    "SAS": "San Antonio Spurs",
    "UTA": "Utah Jazz",
    "GUA": "Guangzhou Loong-Lions"
}

NBA_ARENA_TIMEZONES: Dict[str, ZoneInfo] = {
    "American Airlines Center": ZoneInfo("America/Chicago"),
    "Ball Arena": ZoneInfo("America/Denver"),
    "Barclays Center": ZoneInfo("America/New_York"),
    "Capital One Arena": ZoneInfo("America/New_York"),
    "Chase Center": ZoneInfo("America/Los_Angeles"),
    "Crypto.com Arena": ZoneInfo("America/Los_Angeles"),
    "Delta Center": ZoneInfo("America/Denver"),
    "FedExForum": ZoneInfo("America/Chicago"),
    "Fiserv Forum": ZoneInfo("America/Chicago"),
    "Footprint Center": ZoneInfo("America/Phoenix"),  # Handles Arizona's no-DST rule
    "Frost Bank Center": ZoneInfo("America/Chicago"),
    "Gainbridge Fieldhouse": ZoneInfo("America/New_York"),
    "Golden 1 Center": ZoneInfo("America/Los_Angeles"),
    "Intuit Dome": ZoneInfo("America/Los_Angeles"),
    "Kaseya Center": ZoneInfo("America/New_York"),
    "Kia Center": ZoneInfo("America/New_York"),
    "Little Caesars Arena": ZoneInfo("America/New_York"),
    "Madison Square Garden": ZoneInfo("America/New_York"),
    "Moda Center": ZoneInfo("America/Los_Angeles"),
    "Paycom Center": ZoneInfo("America/Chicago"),
    "Rocket Mortgage FieldHouse": ZoneInfo("America/New_York"),
    "Scotiabank Arena": ZoneInfo("America/Toronto"),  # Matches Eastern Time rules for Canada
    "Smoothie King Center": ZoneInfo("America/Chicago"),
    "Spectrum Center": ZoneInfo("America/New_York"),
    "State Farm Arena": ZoneInfo("America/New_York"),
    "Target Center": ZoneInfo("America/Chicago"),
    "TD Garden": ZoneInfo("America/New_York"),
    "Toyota Center": ZoneInfo("America/Chicago"),
    "United Center": ZoneInfo("America/Chicago"),
    "Wells Fargo Center": ZoneInfo("America/New_York"),
    "Xfinity Mobile Arena": ZoneInfo("America/New_York"),
    "Rocket Arena": ZoneInfo("America/New_York"),
    "crypto.com Arena": ZoneInfo("America/Los_Angeles"),
    "Toyota Center (Houston)": ZoneInfo("America/Chicago"),
    "T-Mobile Arena": ZoneInfo("America/Los_Angeles"),
    "Moody Center": ZoneInfo("America/Chicago"),
    "Mortgage Matchup Center": ZoneInfo("America/Phoenix"),
    "Accor Arena": ZoneInfo("Europe/Paris"),
    "Arena CDMX": ZoneInfo("America/Mexico_City"),
    "The O2": ZoneInfo("Europe/London"),
    "Uber Arena": ZoneInfo("Europe/Berlin"),
}


def arena_timezone(venue_name: str) -> Optional[str]:
    timezone = NBA_ARENA_TIMEZONES.get(venue_name)
    if timezone is not None:
        return str(timezone)

    normalized = venue_name.casefold()
    for name, zone in NBA_ARENA_TIMEZONES.items():
        if name.casefold() == normalized:
            return str(zone)

    return None

def setup_logging():
    logging.config.dictConfig(LOGGING_CONFIG)