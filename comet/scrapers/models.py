from typing import List, Optional, TypedDict

from pydantic import BaseModel


class ScrapeRequest(BaseModel):
    media_type: str  # "movie" or "series"
    media_id: str  # Full ID (e.g., "tt1234567:1:1" or "kitsu:123")
    media_only_id: str  # Base ID (e.g., "tt1234567")
    title: str
    year: Optional[int] = None
    year_end: Optional[int] = None
    season: Optional[int] = None
    episode: Optional[int] = None
    air_date: Optional[str] = None  # ISO date (e.g. "2025-01-06") for date-based searches
    context: str = "live"  # "live" or "background"


class ScrapeResult(TypedDict):
    title: str
    infoHash: str
    fileIndex: Optional[int]
    seeders: Optional[int]
    size: Optional[int]
    tracker: str
    sources: List[str]
