-- Season dimension, derived from the matches actually present in the lake
-- rather than hard-coded, so a new season appears without a code change.

with matches as (

    select * from {{ ref('stg_matches') }}

),

seasons as (

    select
        season,
        min(kickoff_at)              as season_start_at,
        max(kickoff_at)              as season_end_at,
        count(*)                     as match_count,
        count(distinct home_team_code) as team_count

    from matches
    group by season

),

final as (

    select
        {{ surrogate_key(['season']) }}  as season_key,
        season,
        season_start_at,
        season_end_at,
        match_count,
        team_count,
        cast(substr(season, 1, 4) as integer) as start_year

    from seasons

)

select * from final
