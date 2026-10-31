-- Reference data for the team dimension.

select
    team_code,
    team_name,
    city

from {{ source('bronze', 'teams') }}
where team_code is not null
