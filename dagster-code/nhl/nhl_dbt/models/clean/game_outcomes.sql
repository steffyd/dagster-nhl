WITH ranked_teams AS (
  SELECT
    game_id,
    team,
    goals,
    ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY goals DESC) as rank
  FROM {{source('nhl_ingestion', 'raw_game_data')}}
),
no_ties as (
    select gd1.game_id
    from {{source('nhl_ingestion', 'raw_game_data')}} gd1
    join {{source('nhl_ingestion', 'raw_game_data')}} gd2 on gd1.game_id = gd2.game_id
    where gd1.goals <> gd2.goals
)
SELECT
  rt.game_id,
  rt.team,
  case when nt.game_id is null then 'tied'
    else case rank
      when 1 then 'winner'
       else 'loser' end
  end as team_type,
  rt.goals
FROM ranked_teams rt
left join no_ties nt on rt.game_id = nt.game_id
group by rt.game_id, rt.team, case when nt.game_id is null then 'tied'
    else case rank
      when 1 then 'winner'
       else 'loser' end
  end, rt.goals
order by rt.game_id, team_type