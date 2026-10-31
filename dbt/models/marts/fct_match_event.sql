-- Event fact. Grain: one row per in-match event.
-- Partition-friendly on season; this is the largest table in the project and
-- the one that justifies Parquet and partition pruning upstream.

{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='delete+insert'
) }}

with events as (

    select * from {{ ref('stg_match_events') }}

    {% if is_incremental() %}
      where season >= (select coalesce(max(season), '{{ var("earliest_season") }}') from {{ this }})
    {% endif %}

),

teams as (

    select * from {{ ref('dim_team') }}

),

final as (

    select
        e.event_id,
        e.match_id,
        e.season,
        t.team_key,
        e.team_code,
        e.event_minute,
        e.event_type,
        e.player_id,

        case
            when e.event_minute <= 45 then 'first_half'
            when e.event_minute <= 90 then 'second_half'
            else 'stoppage'
        end                                   as match_phase,

        case when e.event_type = 'goal' then 1 else 0 end         as is_goal,
        case when e.event_type like 'shot%' then 1 else 0 end     as is_shot,
        case when e.event_type in ('yellow_card', 'red_card') then 1 else 0 end as is_card

    from events e
    left join teams t on e.team_code = t.team_code

)

select * from final
