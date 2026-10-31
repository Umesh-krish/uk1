-- Team dimension. Surrogate key so facts never join on a natural code that a
-- source system might reuse or rebrand mid-season.

with teams as (

    select * from {{ ref('stg_teams') }}

),

final as (

    select
        {{ surrogate_key(['team_code']) }}  as team_key,
        team_code,
        team_name,
        city

    from teams

)

select * from final
