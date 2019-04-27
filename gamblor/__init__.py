import os

from sqlalchemy import Text, Integer, Float

MIN_YEAR = 2014

NUM_TEAMS = 18

DATA_DIR = "./data"
if not os.path.isdir(DATA_DIR):
    os.mkdir(DATA_DIR)

SCORE_DIR = os.path.join(DATA_DIR,
                         "scores")
if not os.path.isdir(SCORE_DIR):
    os.mkdir(SCORE_DIR)

LADDER_DIR = os.path.join(DATA_DIR,
                          "ladder")
if not os.path.isdir(LADDER_DIR):
    os.mkdir(LADDER_DIR)

ODDS_DIR = os.path.join(DATA_DIR,
                          "odds")
if not os.path.isdir(ODDS_DIR):
    os.mkdir(ODDS_DIR)

STATS_DB = "stats.db"
STATS_DB_PATH = os.path.join(DATA_DIR,
                             STATS_DB)
STATS_CONN = "sqlite:///" + STATS_DB_PATH

SCORES_TABLE_COLUMNS = ["Year", "Round", "GameType", "Venue", "GameTime",
                        "HomeTeam", "AwayTeam", "HomeFinalScore", "AwayFinalScore",
                        "HomeQ1Goals", "HomeQ1Points", "HomeQ2Goals", "HomeQ2Points",
                        "HomeQ3Goals", "HomeQ3Points", "HomeQ4Goals", "HomeQ4Points",
                        "AwayQ1Goals", "AwayQ1Points", "AwayQ2Goals", "AwayQ2Points",
                        "AwayQ3Goals", "AwayQ3Points", "AwayQ4Goals", "AwayQ4Points"]

LUIGI_SCORES_TABLE_COLUMNS = [(["MatchID", Integer()], {"primary_key": True}),
                              (["Year", Integer()], {}),
                              (["Round", Integer()], {}),
                              (["GameType", Text()], {}),
                              (["Venue", Text()], {}),
                              (["GameTime", Text()], {}),
                              (["HomeTeam", Text()], {}),
                              (["AwayTeam", Text()], {}),
                              (["HomeFinalScore", Integer()], {}),
                              (["AwayFinalScore", Integer()], {}),
                              (["HomeQ1Goals", Integer()], {}),
                              (["HomeQ1Points", Integer()], {}),
                              (["HomeQ2Goals", Integer()], {}),
                              (["HomeQ2Points", Integer()], {}),
                              (["HomeQ3Goals", Integer()], {}),
                              (["HomeQ3Points", Integer()], {}),
                              (["HomeQ4Goals", Integer()], {}),
                              (["HomeQ4Points", Integer()], {}),
                              (["AwayQ1Goals", Integer()], {}),
                              (["AwayQ1Points", Integer()], {}),
                              (["AwayQ2Goals", Integer()], {}),
                              (["AwayQ2Points", Integer()], {}),
                              (["AwayQ3Goals", Integer()], {}),
                              (["AwayQ3Points", Integer()], {}),
                              (["AwayQ4Goals", Integer()], {}),
                              (["AwayQ4Points", Integer()], {})]

LADDER_TABLE_COLUMNS = ["Year", "Round", "Team", "GamesPlayed", "Points", "Percentage"]

LUIGI_LADDER_TABLE_COLUMNS = [(["Year", Integer()], {"primary_key": True}),
                              (["Round", Integer()], {"primary_key": True}),
                              (["Team", Text()], {"primary_key": True}),
                              (["GamesPlayed", Integer()], {}),
                              (["Points", Integer()], {}),
                              (["Percentage", Float()], {})
                             ]

ODDS_TABLE_COLUMNS = ["Year", "Round", "GameTime", "Team", "Odds"]

LUIGI_ODDS_TABLE_COLUMNS = [(["MatchID", Integer()], {"primary_key": True}),
                            (["Year", Integer()], {}),
                            (["Round", Integer()], {}),
                            (["GameTime", Text()], {}),
                            (["Team", Text()], {"primary_key": True}),
                            (["Odds", Float()], {}),
                             ]

AFL_TABLES_URL = "https://afltables.com/afl/seas/"

ODDS_URL_DICT = {2011: "http://www.betfair.com.au/hub/wp-content/uploads/sites/2/2018/06/AFL-2011-2016.xlsx",
                 2012: "http://www.betfair.com.au/hub/wp-content/uploads/sites/2/2018/06/AFL-2011-2016.xlsx",
                 2013: "http://www.betfair.com.au/hub/wp-content/uploads/sites/2/2018/06/AFL-2011-2016.xlsx",
                 2014: "http://www.betfair.com.au/hub/wp-content/uploads/sites/2/2018/06/AFL-2011-2016.xlsx",
                 2015: "http://www.betfair.com.au/hub/wp-content/uploads/sites/2/2018/06/AFL-2011-2016.xlsx",
                 2016: "http://www.betfair.com.au/hub/wp-content/uploads/sites/2/2018/06/AFL-2011-2016.xlsx",
                 2017: "http://www.betfair.com.au/hub/wp-content/uploads/sites/2/2019/03/AFL-Data-Dump-2017.xlsx",
                 2018: "http://www.betfair.com.au/hub/wp-content/uploads/sites/2/2019/03/AFL-Data-Dump-2018-2.xlsx"
                }

CHROME_USER_AGENT = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36'} 
