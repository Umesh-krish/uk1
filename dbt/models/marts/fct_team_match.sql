-- Team-perspective fact. Grain: one row per team per match, so every match
-- appears twice.
--
-- This is the model that makes league tables, form guides and home/away splits
-- a simple group-by instead of a union written by hand in every query.

with matches as (

    select * from {{ ref('fct_match') }}

),

home_side as (

    select
        match_id,
        season_key,
        season,
        matchday,
        match_date,
        home_team_key            as team_key,
        home_team_code           as team_code,
        away_team_key            as opponent_team_key,
        away_team_code           as opponent_team_code,
        'home'                   as venue,
        home_goals               as goals_for,
        away_goals               as goals_against,
        home_shots               as shots_for,
        away_shots               as shots_against,
        home_xg                  as xg_for,
        away_xg                  as xg_against
    from matches

),

away_side as (

    select
        match_id,
        season_key,
        season,
        matchday,
        match_date,
        away_team_key,
        away_team_code,
        home_team_key,
        home_team_code,
        'away',
        away_goals,
        home_goals,
        away_shots,
        home_shots,
        away_xg,
        home_xg
    from matches

),

unioned as (

    select * from home_side
    union all
    select * from away_side

),

final as (

    select
        {{ surrogate_key(['match_id', 'team_code']) }} as team_match_key,
        match_id,
        season_key,
        season,
        matchday,
        match_date,
        team_key,
        team_code,
        opponent_team_key,
        opponent_team_code,
        venue,
        goals_for,
        goals_against,
        goals_for - goals_against                   as goal_difference,
        shots_for,
        shots_against,
        xg_for,
        xg_against,

        case
            when goals_for > goals_against then 'win'
            when goals_for < goals_against then 'loss'
            else 'draw'
        end                                         as outcome,

        case
            when goals_for > goals_against then 3
            when goals_for = goals_against then 1
            else 0
        end                                         as points,

        case when goals_against = 0 then 1 else 0 end as is_clean_sheet

    from unioned

)

select * from final
