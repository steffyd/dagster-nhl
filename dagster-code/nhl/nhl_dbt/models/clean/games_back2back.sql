select
    rsd.game_id,
    rsd.team,
    case when rsd.game_id = lag(rsd.game_id, 1) over (partition by rsd.team order by rsd.game_id) then 'back_to_back'
        else 'not_back_to_back' end as back_to_back,
    got.team_type
from {{source('nhl_ingestion', 'raw_schedule_data')}} rsd
-- join to game outcomes to get which team won
join {{ref('game_outcomes')}} got on rsd.game_id = got.game_id
{% if is_incremental() %}
where rsd.partition_key = '{{ var('datetime_to_process') }}'
{% endif %}