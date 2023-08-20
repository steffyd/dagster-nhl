import requests
import pandas as pd

BASE_URL = "https://statsapi.web.nhl.com/api/v1/"


def get_schedule_expanded(date, context):
    url = f"{BASE_URL}schedule?date={date}"
    response = requests.get(url)
    data = response.json()
    schedule = []
    if data["dates"] is None:
        return pd.DataFrame()

    for date_data in data["dates"]:
        for game in date_data["games"]:
            schedule.append(
                {
                    "game_id": game["gamePk"],
                    "game_date": pd.to_datetime(date_data["date"]),
                    "home_team": game["teams"]["home"]["team"]["name"],
                    "home_score" : game["teams"]["home"]["score"],
                    "home_wins_date": game["teams"]["home"]["leagueRecord"]["wins"],
                    "home_losses_date": game["teams"]["home"]["leagueRecord"]["losses"],
                    "home_ties_date": game["teams"]["home"]["leagueRecord"].get(
                        "ties", 0
                    ),
                    "away_team": game["teams"]["away"]["team"]["name"],
                    "away_score" : game["teams"]["away"]["score"],
                    "away_wins_date": game["teams"]["away"]["leagueRecord"]["wins"],
                    "away_losses_date": game["teams"]["away"]["leagueRecord"]["losses"],
                    "away_ties_date": game["teams"]["away"]["leagueRecord"].get(
                        "ties", 0
                    ),
                    "venue": game["venue"]["name"],
                    "game_type": game["gameType"],
                    "season": game["season"],
                }
            )

    return pd.DataFrame(schedule)


def get_schedule(date, context):
    url = f"{BASE_URL}schedule?date={date}"
    response = requests.get(url)
    data = response.json()
    schedule = []
    if data["dates"] is None:
        return pd.DataFrame()

    for date_data in data["dates"]:
        for game in date_data["games"]:
            schedule.append(
                {
                    "game_id": game["gamePk"],
                    "game_date": date_data["date"],
                    "home_team": game["teams"]["home"]["team"]["name"],
                    "away_team": game["teams"]["away"]["team"]["name"],
                    "game_type": game["gameType"],
                    "season": game["season"],
                }
            )

    return pd.DataFrame(schedule)


def get_team_stats(game_id):
    url = f"{BASE_URL}game/{game_id}/boxscore"
    response = requests.get(url)
    data = response.json()

    home = data["teams"]["home"]["teamStats"]["teamSkaterStats"]
    away = data["teams"]["away"]["teamStats"]["teamSkaterStats"]
    home["team"] = data["teams"]["home"]["team"]["name"]
    away["team"] = data["teams"]["away"]["team"]["name"]
    home["team_type"] = "home"
    away["team_type"] = "away"

    boxscore = pd.DataFrame([home, away])
    column_name_mapping = {
        "faceOffWinPercentage": "faceoff_pct",
        "powerPlayOpportunities": "pp_opp",
        "powerPlayGoals": "pp_goals",
        "powerPlayPercentage": "pp_pct",
    }

    # Rename the columns in-place
    boxscore.rename(columns=column_name_mapping, inplace=True)
    boxscore["game_id"] = game_id

    return boxscore
