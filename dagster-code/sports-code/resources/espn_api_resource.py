import requests
from dagster import ConfigurableResource

class EspnApiResource(ConfigurableResource):
    _base_url = "http://site.api.espn.com/apis/site/v2/sports/"

    # http://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard
    # http://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard
    # http://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard
    # http://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard
    def get_scoreboard_for_date(self, sport, league, date):
        return requests.get(f"{self._base_url}/{sport}/{league}/scoreboard?dates={date}").json()