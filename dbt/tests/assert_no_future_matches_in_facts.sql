-- A match dated in the future means a bad parse of the kickoff column, which is
-- the most common failure when a source changes its date format mid-season.

select
    match_id,
    kickoff_at

from {{ ref('fct_match') }}
where kickoff_at > current_timestamp + interval 1 day
