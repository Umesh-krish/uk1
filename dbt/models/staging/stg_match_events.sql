-- Bronze events, typed. One row per event.
-- The streaming and batch paths both land here; `source_path` records which,
-- so the reconciliation test can tell a missing live event from a missing
-- historical one.

with source as (

    select * from {{ source('bronze', 'events') }}

),

renamed as (

    select
        event_id,
        match_id,
        season,
        cast(minute as integer)   as event_minute,
        team_code,
        lower(event_type)         as event_type,
        player_id

    from source
    where event_id is not null
      and match_id is not null

)

select * from renamed
