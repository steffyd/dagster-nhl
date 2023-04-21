with ranked_teams as (
  select
    game_id,
    team,
    goals,
    row_number() over (partition by game_id order by goals desc) as rank
  from {{source('nhl_ingestion', 'raw_game_data')}}
  {% if is_incremental() %}
  where partition_key = '{{ var('datetime_to_process') }}'
  {% endif %}
),
no_ties as (
    select gd1.game_id
    from ranked_teams gd1
    join ranked_teams gd2 on gd1.game_id = gd2.game_id and gd1.team <> gd2.team
    where gd1.goals <> gd2.goals
)
select
  rt.game_id,
  rt.team,
  case when nt.game_id is null then 'tied'
    else case rank
      when 1 then 'winner'
       else 'loser' end
  end as team_type,
  rt.goals
from ranked_teams rt
left join no_ties nt on rt.game_id = nt.game_id
group by rt.game_id, rt.team, case when nt.game_id is null then 'tied'
    else case rank
      when 1 then 'winner'
       else 'loser' end
  end, rt.goals
order by rt.game_id, team_type