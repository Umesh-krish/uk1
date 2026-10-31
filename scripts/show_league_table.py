"""Print the gold-layer league table, as a quick check that the run worked."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import duckdb

DB = Path(os.getenv("FOOTBALL_DATA", "data")) / "warehouse" / "football.duckdb"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", default=None, help="Defaults to the most recent season present")
    ap.add_argument("--limit", type=int, default=10)
    args = ap.parse_args()

    if not DB.exists():
        sys.exit(f"{DB} not found; run `make warehouse` first")

    con = duckdb.connect(str(DB), read_only=True)
    try:
        season = args.season or con.execute(
            "select max(season) from main_marts.agg_team_season_form"
        ).fetchone()[0]

        rows = con.execute(
            """
            select league_position, team_code, matches_played, wins, draws, losses,
                   goals_for, goals_against, goal_difference, points,
                   points_per_game, finishing_above_xg
            from main_marts.agg_team_season_form
            where season = ?
            order by league_position
            limit ?
            """,
            [season, args.limit],
        ).fetchall()
    finally:
        con.close()

    header = f"{'#':<4}{'TEAM':<7}{'P':<5}{'W':<4}{'D':<4}{'L':<4}{'GF':<5}{'GA':<5}{'GD':<6}{'PTS':<6}{'PPG':<7}{'xG +/-'}"
    print(f"\n  {season} league table\n")
    print("  " + header)
    print("  " + "-" * len(header))
    for r in rows:
        print(
            f"  {r[0]:<4}{r[1]:<7}{r[2]:<5}{r[3]:<4}{r[4]:<4}{r[5]:<4}"
            f"{r[6]:<5}{r[7]:<5}{r[8]:<+6}{r[9]:<6}{r[10]:<7}{r[11]:+}"
        )
    print()


if __name__ == "__main__":
    main()
