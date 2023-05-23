{{
    config(
        materialized='view'
    )
}}
select
 sd.game_id,
 sd.home_score > sd.away_score as target,
 dts.*
from {{ref('team_season_totals_by_date')}} dts
join {{ref('season_data')}} sd on dts.game_id = sd.game_id
where game_type = 'R'