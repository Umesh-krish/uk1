-- Gold aggregate. Grain: one row per team per season.
-- This is the table a BI tool or an API would actually read: the league table,
-- plus the home/away split and the xG gap that the raw fixtures cannot show.

with team_matches as (

    select * from {{ ref('fct_team_match') }}

),

aggregated as (

    select
        season,
        team_key,
        team_code,

        count(*)                                                  as matches_played,
        sum(points)                                               as points,
        sum(case when outcome = 'win'  then 1 else 0 end)          as wins,
        sum(case when outcome = 'draw' then 1 else 0 end)          as draws,
        sum(case when outcome = 'loss' then 1 else 0 end)          as losses,

        sum(goals_for)                                            as goals_for,
        sum(goals_against)                                        as goals_against,
        sum(goals_for) - sum(goals_against)                       as goal_difference,
        sum(is_clean_sheet)                                       as clean_sheets,

        sum(case when venue = 'home' then points else 0 end)       as home_points,
        sum(case when venue = 'away' then points else 0 end)       as away_points,

        round(sum(xg_for), 2)                                     as xg_for,
        round(sum(xg_against), 2)                                 as xg_against,
        round(sum(goals_for) - sum(xg_for), 2)                    as finishing_above_xg,

        round(cast(sum(points) as double) / nullif(count(*), 0), 2) as points_per_game

    from team_matches
    group by season, team_key, team_code

),

ranked as (

    select
        *,
        row_number() over (
            partition by season
            order by points desc, goal_difference desc, goals_for desc
        ) as league_position

    from aggregated

)

select * from ranked
