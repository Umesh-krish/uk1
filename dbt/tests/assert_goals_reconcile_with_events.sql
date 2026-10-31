-- The reconciliation test that earns its keep.
--
-- The scoreline on fct_match comes from the match feed. The goal events on
-- fct_match_event come from the event feed, often a different system with a
-- different latency. If the two disagree, something upstream dropped or
-- duplicated events, and every in-play metric built on events is wrong while
-- the league table still looks fine.
--
-- Returns the offending matches. Any row returned fails the test.

with scoreline as (

    select
        match_id,
        home_goals + away_goals as goals_on_match

    from {{ ref('fct_match') }}

),

from_events as (

    select
        match_id,
        sum(is_goal) as goals_from_events

    from {{ ref('fct_match_event') }}
    group by match_id

)

select
    s.match_id,
    s.goals_on_match,
    coalesce(e.goals_from_events, 0) as goals_from_events,
    s.goals_on_match - coalesce(e.goals_from_events, 0) as difference

from scoreline s
left join from_events e on s.match_id = e.match_id
where s.goals_on_match <> coalesce(e.goals_from_events, 0)
