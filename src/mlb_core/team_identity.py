from __future__ import annotations

TEAM_IDENTITIES = [
    {"team_id": 111, "canonical_name": "Boston Red Sox", "canonical_abbreviation": "BOS", "league": "AL", "division": "AL East"},
    {"team_id": 147, "canonical_name": "New York Yankees", "canonical_abbreviation": "NYY", "league": "AL", "division": "AL East"},
    {"team_id": 139, "canonical_name": "Tampa Bay Rays", "canonical_abbreviation": "TB", "league": "AL", "division": "AL East"},
    {"team_id": 110, "canonical_name": "Baltimore Orioles", "canonical_abbreviation": "BAL", "league": "AL", "division": "AL East"},
    {"team_id": 141, "canonical_name": "Toronto Blue Jays", "canonical_abbreviation": "TOR", "league": "AL", "division": "AL East"},
    {"team_id": 114, "canonical_name": "Cleveland Guardians", "canonical_abbreviation": "CLE", "league": "AL", "division": "AL Central"},
    {"team_id": 116, "canonical_name": "Detroit Tigers", "canonical_abbreviation": "DET", "league": "AL", "division": "AL Central"},
    {"team_id": 118, "canonical_name": "Kansas City Royals", "canonical_abbreviation": "KC", "league": "AL", "division": "AL Central"},
    {"team_id": 145, "canonical_name": "Chicago White Sox", "canonical_abbreviation": "CWS", "league": "AL", "division": "AL Central"},
    {"team_id": 142, "canonical_name": "Minnesota Twins", "canonical_abbreviation": "MIN", "league": "AL", "division": "AL Central"},
    {"team_id": 117, "canonical_name": "Houston Astros", "canonical_abbreviation": "HOU", "league": "AL", "division": "AL West"},
    {"team_id": 108, "canonical_name": "Los Angeles Angels", "canonical_abbreviation": "LAA", "league": "AL", "division": "AL West"},
    {"team_id": 133, "canonical_name": "Athletics", "canonical_abbreviation": "ATH", "league": "AL", "division": "AL West"},
    {"team_id": 136, "canonical_name": "Seattle Mariners", "canonical_abbreviation": "SEA", "league": "AL", "division": "AL West"},
    {"team_id": 140, "canonical_name": "Texas Rangers", "canonical_abbreviation": "TEX", "league": "AL", "division": "AL West"},
    {"team_id": 144, "canonical_name": "Atlanta Braves", "canonical_abbreviation": "ATL", "league": "NL", "division": "NL East"},
    {"team_id": 146, "canonical_name": "Miami Marlins", "canonical_abbreviation": "MIA", "league": "NL", "division": "NL East"},
    {"team_id": 121, "canonical_name": "New York Mets", "canonical_abbreviation": "NYM", "league": "NL", "division": "NL East"},
    {"team_id": 143, "canonical_name": "Philadelphia Phillies", "canonical_abbreviation": "PHI", "league": "NL", "division": "NL East"},
    {"team_id": 120, "canonical_name": "Washington Nationals", "canonical_abbreviation": "WSH", "league": "NL", "division": "NL East"},
    {"team_id": 112, "canonical_name": "Chicago Cubs", "canonical_abbreviation": "CHC", "league": "NL", "division": "NL Central"},
    {"team_id": 113, "canonical_name": "Cincinnati Reds", "canonical_abbreviation": "CIN", "league": "NL", "division": "NL Central"},
    {"team_id": 158, "canonical_name": "Milwaukee Brewers", "canonical_abbreviation": "MIL", "league": "NL", "division": "NL Central"},
    {"team_id": 134, "canonical_name": "Pittsburgh Pirates", "canonical_abbreviation": "PIT", "league": "NL", "division": "NL Central"},
    {"team_id": 138, "canonical_name": "St. Louis Cardinals", "canonical_abbreviation": "STL", "league": "NL", "division": "NL Central"},
    {"team_id": 109, "canonical_name": "Arizona Diamondbacks", "canonical_abbreviation": "AZ", "league": "NL", "division": "NL West"},
    {"team_id": 115, "canonical_name": "Colorado Rockies", "canonical_abbreviation": "COL", "league": "NL", "division": "NL West"},
    {"team_id": 119, "canonical_name": "Los Angeles Dodgers", "canonical_abbreviation": "LAD", "league": "NL", "division": "NL West"},
    {"team_id": 135, "canonical_name": "San Diego Padres", "canonical_abbreviation": "SD", "league": "NL", "division": "NL West"},
    {"team_id": 137, "canonical_name": "San Francisco Giants", "canonical_abbreviation": "SF", "league": "NL", "division": "NL West"},
]

TEAM_IDENTITY_BY_ID = {row["team_id"]: row for row in TEAM_IDENTITIES}


def get_team_identity(team_id: int) -> dict:
    identity = TEAM_IDENTITY_BY_ID.get(int(team_id))
    if identity is None:
        raise KeyError(f"Unknown canonical team_id: {team_id}")
    return identity
