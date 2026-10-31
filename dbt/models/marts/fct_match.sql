-- Match fact. Grain: one row per match.
-- Incremental so a nightly run reprocesses only the current season; a full
-- rebuild is still one `dbt run --full-refresh` away.

{{ config(
    materialized='incremental',
    unique_key='match_id',
    incremental_strategy='delete+insert'
) }}

with matches as (

    select * from {{ ref('stg_matches') }}
    where season >= '{{ var("earliest_season") }}'

    {% if is_incremental() %}
      and season >= (select coalesce(max(season), '{{ var("earliest_season") }}') from {{ this }})
    {% endif %}

),

teams as (

    select * from {{ ref('dim_team') }}

),

seasons as (

    select * from {{ ref('dim_season') }}

),

final as (

    select
        m.match_id,
        s.season_key,
        m.season,
        m.matchday,
        m.kickoff_at,
        cast(m.kickoff_at as date)              as match_date,

        home.team_key                           as home_team_key,
        away.team_key                           as away_team_key,
        m.home_team_code,
        m.away_team_code,

        m.home_goals,
        m.away_goals,
        m.home_goals + m.away_goals             as total_goals,
        m.home_goals - m.away_goals             as goal_difference,

        case
            when m.home_goals > m.away_goals then 'home_win'
            when m.home_goals < m.away_goals then 'away_win'
            else 'draw'
        end                                     as result,

        m.home_shots,
        m.away_shots,
        m.home_corners,
        m.away_corners,
        m.home_fouls,
        m.away_fouls,
        m.home_xg,
        m.away_xg,

        -- How far the scoreline ran ahead of the underlying chances. Positive
        -- means the home side outscored its xG.
        round((m.home_goals - m.home_xg) - (m.away_goals - m.away_xg), 2) as xg_overperformance,

        m.attendance,
        m.referee

    from matches m
    left join teams   home on m.home_team_code = home.team_code
    left join teams   away on m.away_team_code = away.team_code
    left join seasons s    on m.season = s.season

)

select * from final
