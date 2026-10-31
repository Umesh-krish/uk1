-- Each club should appear the same number of times in a completed season, half
-- at home and half away. An unbalanced count means a fixture was dropped during
-- ingestion, which quietly distorts every per-team aggregate downstream.
--
-- Only completed seasons are checked; the current one is still in progress.

with per_team as (

    select
        season,
        team_code,
        count(*)                                            as played,
        sum(case when venue = 'home' then 1 else 0 end)     as home_games,
        sum(case when venue = 'away' then 1 else 0 end)     as away_games

    from {{ ref('fct_team_match') }}
    group by season, team_code

),

season_mode as (

    select
        season,
        max(played) as expected_games

    from per_team
    group by season

)

select
    p.season,
    p.team_code,
    p.played,
    p.home_games,
    p.away_games,
    m.expected_games

from per_team p
join season_mode m on p.season = m.season
where p.played <> m.expected_games
   or p.home_games <> p.away_games
