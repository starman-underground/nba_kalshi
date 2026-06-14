from dataclasses import dataclass
from datetime import datetime
from typing import List

@dataclass
class GameWinnerMarketMetadata:
    ticker: str
    title: str
    rules_primary: str
    event_ticker: str
    result: ["yes", "no"]
    volume: float
    open_ts: datetime
    settlement_ts: datetime
    expected_expiration_time: datetime
    expiration_ts: datetime
    teams: List[str]
    yes_winner: str
    no_winner: str