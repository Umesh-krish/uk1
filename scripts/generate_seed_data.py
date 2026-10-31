"""
Generate realistic football match + event data as the pipeline's source.

In production this script is replaced by `ingestion/extract_matches.py`, which
pulls real season CSVs. This generator exists so the whole stack can be run and
demoed offline, with no API keys and no network.

Writes:
    data/landing/matches/season=<season>/matches.csv
    data/landing/events/season=<season>/events.csv
"""
from __future__ import annotations

import argparse
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

TEAMS = [
    ("ARS", "Arsenal", "London"),
    ("AVL", "Aston Villa", "Birmingham"),
    ("BOU", "Bournemouth", "Bournemouth"),
    ("BRE", "Brentford", "London"),
    ("BHA", "Brighton", "Brighton"),
    ("CHE", "Chelsea", "London"),
    ("CRY", "Crystal Palace", "London"),
    ("EVE", "Everton", "Liverpool"),
    ("FUL", "Fulham", "London"),
    ("LEE", "Leeds United", "Leeds"),
    ("LIV", "Liverpool", "Liverpool"),
    ("MCI", "Manchester City", "Manchester"),
    ("MUN", "Manchester United", "Manchester"),
    ("NEW", "Newcastle United", "Newcastle"),
    ("NFO", "Nottingham Forest", "Nottingham"),
    ("SOU", "Southampton", "Southampton"),
    ("TOT", "Tottenham Hotspur", "London"),
    ("WHU", "West Ham United", "London"),
    ("WOL", "Wolves", "Wolverhampton"),
    ("BUR", "Burnley", "Burnley"),
]

# Rough strength ratings drive scorelines so downstream aggregates look sane.
STRENGTH = {
    "MCI": 92, "LIV": 89, "ARS": 88, "CHE": 82, "MUN": 80, "TOT": 80,
    "NEW": 78, "AVL": 77, "BHA": 75, "WHU": 73, "CRY": 71, "FUL": 70,
    "BRE": 70, "WOL": 68, "EVE": 67, "NFO": 66, "BOU": 66, "LEE": 63,
    "SOU": 62, "BUR": 61,
}

EVENT_TYPES = ["shot", "shot_on_target", "goal", "corner", "foul", "yellow_card", "red_card", "substitution"]
EVENT_WEIGHTS = [34, 16, 0, 18, 20, 7, 1, 4]  # goals are emitted separately

REFEREES = ["M Oliver", "A Taylor", "P Tierney", "S Attwell", "C Pawson", "J Brooks", "D Coote"]


def poisson(lam: float, rng: random.Random) -> int:
    """Knuth's algorithm; avoids a numpy dependency in the seed path."""
    import math

    limit, k, p = math.exp(-lam), 0, 1.0
    while True:
        k += 1
        p *= rng.random()
        if p <= limit:
            return k - 1


def expected_goals(attack: int, defence: int) -> float:
    edge = (attack - defence) / 25.0
    return max(0.25, 1.35 + edge)


def build_fixtures(teams: list[str], rng: random.Random) -> list[tuple[str, str]]:
    """Double round robin: every team plays every other home and away."""
    fixtures = [(h, a) for h in teams for a in teams if h != a]
    rng.shuffle(fixtures)
    return fixtures


def generate_season(season: str, start: date, rng: random.Random):
    codes = [t[0] for t in TEAMS]
    fixtures = build_fixtures(codes, rng)
    matches, events = [], []
    match_day = start

    for idx, (home, away) in enumerate(fixtures, start=1):
        if idx % 10 == 1:
            match_day += timedelta(days=rng.choice([3, 4, 7]))

        home_xg = expected_goals(STRENGTH[home] + 4, STRENGTH[away])  # home advantage
        away_xg = expected_goals(STRENGTH[away], STRENGTH[home] + 4)
        home_goals, away_goals = poisson(home_xg, rng), poisson(away_xg, rng)

        kickoff = datetime.combine(match_day, datetime.min.time()) + timedelta(
            hours=rng.choice([12, 15, 17, 20]), minutes=rng.choice([0, 30])
        )
        match_id = f"{season}-{idx:04d}"

        matches.append(
            {
                "match_id": match_id,
                "season": season,
                "matchday": (idx - 1) // 10 + 1,
                "kickoff_utc": kickoff.isoformat(timespec="seconds"),
                "home_team_code": home,
                "away_team_code": away,
                "home_goals": home_goals,
                "away_goals": away_goals,
                "home_shots": home_goals * 3 + poisson(7, rng),
                "away_shots": away_goals * 3 + poisson(6, rng),
                "home_corners": poisson(5.2, rng),
                "away_corners": poisson(4.6, rng),
                "home_fouls": poisson(10.5, rng),
                "away_fouls": poisson(11.0, rng),
                "attendance": rng.randint(21000, 73000),
                "referee": rng.choice(REFEREES),
                "home_xg": round(home_xg + rng.uniform(-0.4, 0.4), 2),
                "away_xg": round(away_xg + rng.uniform(-0.4, 0.4), 2),
            }
        )

        # Minute-level events. Goals are placed first so they always reconcile
        # with the scoreline on the match row; that reconciliation is the point
        # of the fct_match_event vs fct_match data quality test downstream.
        minutes = sorted(rng.sample(range(1, 91), k=min(90, home_goals + away_goals)))
        for i, minute in enumerate(minutes):
            team = home if i < home_goals else away
            events.append(
                {
                    "event_id": f"{match_id}-G{i:02d}",
                    "match_id": match_id,
                    "season": season,
                    "minute": minute,
                    "team_code": team,
                    "event_type": "goal",
                    "player_id": f"P{rng.randint(1000, 9999)}",
                }
            )

        for j in range(poisson(26, rng)):
            team = rng.choice([home, away])
            events.append(
                {
                    "event_id": f"{match_id}-E{j:03d}",
                    "match_id": match_id,
                    "season": season,
                    "minute": rng.randint(1, 95),
                    "team_code": team,
                    "event_type": rng.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0],
                    "player_id": f"P{rng.randint(1000, 9999)}",
                }
            )

    return matches, events


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--seasons", nargs="*", default=["2023-24", "2024-25", "2025-26"])
    ap.add_argument("--out", default="data/landing")
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    out = Path(args.out)

    teams_rows = [{"team_code": c, "team_name": n, "city": city} for c, n, city in TEAMS]
    write_csv(out / "teams" / "teams.csv", teams_rows)
    print(f"teams      -> {out/'teams'/'teams.csv'} ({len(teams_rows)} rows)")

    for offset, season in enumerate(args.seasons):
        start_year = 2023 + offset
        matches, events = generate_season(season, date(start_year, 8, 12), rng)
        write_csv(out / "matches" / f"season={season}" / "matches.csv", matches)
        write_csv(out / "events" / f"season={season}" / "events.csv", events)
        print(f"{season}    -> {len(matches):>4} matches, {len(events):>6} events")


if __name__ == "__main__":
    main()
