-- get all back to back games and their outcome
-- using incremental for each day's games
select
    game_id,
    team,
    case when game_id = lag(game_id, 1) over (partition by team order by game_id) then 'back_to_back'
        else 'not_back_to_back' end as back_to_back,
    case when game_id = lag(game_id, 1) over (partition by team order by game_id) then lag(goals, 1) over (partition by team order by game_id)
        else null end as goals
from {{source('nhl_ingestion', 'raw_game_data')}}
{% if is_incremental() %}
where partition_key = '{{ var('datetime_to_process') }}'
{% endif %}