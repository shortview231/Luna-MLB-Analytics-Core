from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://statsapi.mlb.com/api/v1"

_RETRY = Retry(
    total=5,
    backoff_factor=0.6,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
_SESSION = requests.Session()
_SESSION.mount("https://", HTTPAdapter(max_retries=_RETRY))
_SESSION.mount("http://", HTTPAdapter(max_retries=_RETRY))


def fetch_schedule(start_date: str, end_date: str, sport_id: int = 1, game_type: str = "R") -> dict:
    params = {
        "sportId": sport_id,
        "gameType": game_type,
        "startDate": start_date,
        "endDate": end_date,
        "hydrate": "team,linescore",
    }
    r = _SESSION.get(f"{BASE}/schedule", params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def fetch_boxscore(game_pk: int) -> dict:
    r = _SESSION.get(f"{BASE}/game/{game_pk}/boxscore", timeout=60)
    r.raise_for_status()
    return r.json()
