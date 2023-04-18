-- write a query that returns whether each game
-- was a back to back game for each team and whether they won or lost
-- (or tied)
-- use the game_outcomes CTE as a starting point
-- and join it to the game table to get the date
-- and join it to itself to get the previous game
select
    go.game_id,
    go.team,
    go.team_type,
    go.goals,
    g.game_date,
    g2.game_date as prev_game_date,
    case when g2.game_date is null then false
        else g.game_date - g2.game_date < 2 end as back_to_back
from {{ref('game_outcomes')}} go
join {{source('nhl_ingestion', 'raw_game_data')}} g on go.game_id = g.game_id
left join {{source('nhl_ingestion', 'raw_game_data')}} g2 on go.game_id = g2.game_id and g2.game_date < g.game_date
order by go.game_id, go.team_type


 