from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class GameMarketMetadata(BaseModel):
    ticker: str = Field(..., description="The ticker of the market")
    title: str = Field(..., description="The title of the market")
    rules_primary: str = Field(..., description="The primary rules of the market")
    event_ticker: str = Field(..., description="The event ticker of the market")
    result: Literal["yes", "no"] = Field(..., description="The result of the market")
    yes_winner: str = Field(..., description="The winner of the game if the result is yes")
    no_winner: str = Field(..., description="The winner of the game if the result is no")
    volume: float = Field(..., gte=0, description="The volume of the market")
    open_ts: datetime = Field(..., description="The open time of the market")
    settlement_ts: datetime = Field(..., description="The settlement time of the market")
    expected_expiration_time: datetime = Field(..., description="The expected expiration time of the market")
    expiration_ts: datetime = Field(..., description="The expiration time of the market")
    teams: List[str] = Field(..., description="The teams of the game")
    tip_off_ts_utc: Optional[datetime] = Field(
        None,
        description="Scheduled tip-off time in UTC from ESPN via sportsdataverse",
    )
    tip_off_ts_local: Optional[datetime] = Field(
        None,
        description="Scheduled tip-off time in the arena's local timezone (see game_tz)",
    )
    game_tz: Optional[str] = Field(
        None,
        description="IANA timezone of the game venue from ESPN via sportsdataverse",
    )
    arena: Optional[str] = Field(
        None,
        description="Venue full name from ESPN via sportsdataverse",
    )

    @field_validator("teams")
    @classmethod
    def validate_teams(cls, v: List[str]) -> List[str]:
        if len(v) != 2:
            raise ValueError("Teams must be a list of two teams")
        return v

    @model_validator(mode="after")
    def validate_winners_in_teams(self) -> "GameMarketMetadata":
        if self.yes_winner not in self.teams:
            raise ValueError("Yes winner must be one of the teams")
        if self.no_winner not in self.teams:
            raise ValueError("No winner must be one of the teams")
        return self