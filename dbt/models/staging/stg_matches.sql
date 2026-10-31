-- Bronze matches, typed and renamed into the project's naming convention.
-- One row per match. No business logic here by design: staging exists so that
-- every downstream model reads consistent column names and types, and so a
-- source schema change is fixed in exactly one place.

with source as (

    select * from {{ source('bronze', 'matches') }}

),

renamed as (

    select
        match_id,
        season,
        cast(matchday as integer)              as matchday,
        cast(kickoff_utc as timestamp)         as kickoff_at,
        home_team_code,
        away_team_code,
        cast(home_goals as integer)            as home_goals,
        cast(away_goals as integer)            as away_goals,
        cast(home_shots as integer)            as home_shots,
        cast(away_shots as integer)            as away_shots,
        cast(home_corners as integer)          as home_corners,
        cast(away_corners as integer)          as away_corners,
        cast(home_fouls as integer)            as home_fouls,
        cast(away_fouls as integer)            as away_fouls,
        cast(attendance as integer)            as attendance,
        referee,
        cast(home_xg as double)                as home_xg,
        cast(away_xg as double)                as away_xg

    from source
    where match_id is not null

)

select * from renamed
