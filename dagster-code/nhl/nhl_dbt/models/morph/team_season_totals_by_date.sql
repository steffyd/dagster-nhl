{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='game_id'
    )
}}
with game_teams as (
    select distinct
        game_id,
        home_team as team_name,
        season,
        game_date
    from {{ ref('game_data') }}
    {% if is_incremental() %}
    where partition_key = '{{ var('datetime_to_process') }}'
    {% endif %}
    union
    select distinct
        game_id,
        away_team as team_name,
        season,
        game_date
    from {{ ref('game_data') }}
    {% if is_incremental() %}
    where partition_key = '{{ var('datetime_to_process') }}'
    {% endif %}
), total_home_data as (
select
    ts.game_id,
    ts.team_name,
    ts.season,
    count(*) as home_game_count,
    sum(case when hg.away_goals > hg.home_goals then 1 else 0 end) as total_home_wins,
    sum(case when hg.away_goals < hg.home_goals then 1 else 0 end) as total_home_losses,
    sum(case when hg.home_goals = hg.away_goals then 1 else 0 end) as total_home_ties,
    sum(hg.home_goals) as total_home_goals,
    sum(hg.away_goals) as total_home_opp_goals,
    sum(hg.home_pim) as total_home_pims,
    sum(hg.away_pim) as total_home_opp_pims,
    sum(hg.home_shots) as total_home_shots,
    sum(hg.away_shots) as total_home_opp_shots
from game_teams ts
join clean.game_data hg on ts.team_name = hg.home_team and ts.season = hg.season
where hg.game_date < ts.game_date
and hg.game_type <> 'PR'
group by ts.team_name, ts.season, ts.game_id
), total_away_data as (
select
    ts.game_id,
    ts.team_name,
    ts.season,
    count(*) as away_game_count,
    sum(case when ag.away_goals > ag.home_goals then 1 else 0 end) as total_away_wins,
    sum(case when ag.away_goals < ag.home_goals then 1 else 0 end) as total_away_losses,
    sum(case when ag.home_goals = ag.away_goals then 1 else 0 end) as total_away_ties,
    sum(ag.home_goals) as total_away_opp_goals,
    sum(ag.away_goals) as total_away_goals,
    sum(ag.home_pim) as total_away_opp_pims,
    sum(ag.away_pim) as total_away_pims,
    sum(ag.home_shots) as total_away_opp_shots,
    sum(ag.away_shots) as total_away_shots
from game_teams ts
join clean.game_data ag on ts.team_name = ag.away_team and ts.season = ag.season
where ag.game_date < ts.game_date
and ag.game_type <> 'PR'
group by ts.team_name, ts.season, ts.game_id
), season_totals as (
select
        ts.game_id,
        ts.team_name as team_name,
        ts.season as season,
        coalesce(home_game_count,0) + coalesce(away_game_count,0) as total_game_count,
        total_home_wins + total_away_wins as total_wins,
        total_home_losses + total_away_losses as total_losses,
        total_home_ties + total_away_ties as total_ties,
        total_home_goals + total_away_goals as goals_for,
        total_home_opp_goals + total_away_opp_goals as goals_against,
        total_home_pims + total_away_pims as total_pims,
        total_home_opp_pims + total_away_opp_pims as total_opp_pims,
        total_home_shots + total_away_shots as total_shots,
        total_home_opp_shots + total_away_opp_shots as total_opp_shots,
        home_game_count,
        away_game_count,
        total_home_wins,
        total_away_wins,
        total_home_losses,
        total_away_losses,
        total_home_ties,
        total_away_ties,
        total_home_goals,
        total_away_goals,
        total_home_opp_goals,
        total_away_opp_goals,
        total_home_pims,
        total_away_pims,
        total_home_opp_pims,
        total_away_opp_pims,
        total_home_shots,
        total_away_shots,
        total_home_opp_shots,
        total_away_opp_shots
from game_teams ts
join total_home_data thd on thd.game_id = ts.game_id and ts.team_name = thd.team_name
join total_away_data tad on tad.game_id = ts.game_id and ts.team_name = tad.team_name
)
select game_id,
       team_name,
       season,
       total_game_count,
       goals_for - goals_against as goal_diff,
       total_pims - total_opp_pims as pim_diff,
       total_shots - total_opp_shots as shot_diff,
       total_wins,
       total_losses,
       total_ties,
       goals_for,
       goals_against,
       total_pims,
       total_opp_pims,
       total_shots,
       total_opp_shots,
       home_game_count,
       away_game_count,
       total_home_wins,
       total_away_wins,
       total_home_losses,
       total_away_losses,
       total_home_ties,
       total_away_ties,
       total_home_goals,
       total_away_goals,
       total_home_opp_goals,
       total_away_opp_goals,
       total_home_pims,
       total_away_pims,
       total_home_opp_pims,
       total_away_opp_pims,
       total_home_shots,
       total_away_shots,
       total_home_opp_shots,
       total_away_opp_shots
from season_totals