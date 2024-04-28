{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "game_date",
            "data_type": "datetime",
            "granularity": "day"
        }
    )
}}

-- create a cte for each home and away team
-- in the game_id for raw.game_data and flatten
-- the data into a single row
with home as (
    select *
    from {{source('nhl_ingestion', 'game_data_raw')}}
    {% if is_incremental() %}
    where game_date >= '{{ var('min_date') }}'
    and game_date <= '{{ var('max_date') }}'
    and team_type = 'home'
    {% endif %}
    {% if not is_incremental() %}
    where team_type = 'home'
    {% endif %}
), away as (
    select *
    from {{source('nhl_ingestion', 'game_data_raw')}}
    {% if is_incremental() %}
    where game_date >= '{{ var('min_date') }}'
    and game_date <= '{{ var('max_date') }}'
    and team_type = 'away'
    {% endif %}
    {% if not is_incremental() %}
    where team_type = 'away'
    {% endif %}
)
-- join the home and away data on game_id
-- and flatten the data into a single row
select distinct
    home.game_id as game_id,
    home.game_date as game_date,
    home.game_type as game_type,
    home.season as season,
    home.team as home_team,
    home.goals as home_goals,
    home.pim as home_pim,
    home.shots as home_shots,
    parse_numeric(home.pp_pct) as home_pp_pct,
    home.pp_goals as home_pp_goals,
    home.pp_opp as home_pp_opp,
    parse_numeric(home.faceoff_pct) as home_faceoff_pct,
    home.blocked as home_blocked,
    home.takeaways as home_takeaways,
    home.giveaways as home_giveaways,
    home.hits as home_hits,
    away.team as away_team,
    away.goals as away_goals,
    away.pim as away_pim,
    away.shots as away_shots,
    parse_numeric(away.pp_pct) as away_pp_pct,
    away.pp_goals as away_pp_goals,
    away.pp_opp as away_pp_opp,
    parse_numeric(away.faceoff_pct) as away_faceoff_pct,
    away.blocked as away_blocked,
    away.takeaways as away_takeaways,
    away.giveaways as away_giveaways,
    away.hits as away_hits
from home
join away on home.game_id = away.game_id