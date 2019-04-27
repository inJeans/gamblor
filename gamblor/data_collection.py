# -*- coding: utf-8 -*-
"""Example Google style docstrings.

This module demonstrates documentation as specified by the `Google Python
Style Guide`_. Docstrings may extend over multiple lines. Sections are created
with a section header and a colon followed by a block of indented text.

Example:
    Examples can be given using either the ``Example`` or ``Examples``
    sections. Sections support any reStructuredText formatting, including
    literal blocks::

        $ python example_google.py

Section breaks are created by resuming unindented text. Section breaks
are also implicitly created anytime a new section starts.

Attributes:
    module_level_variable1 (int): Module level variables may be documented in
        either the ``Attributes`` section of the module docstring, or in an
        inline docstring immediately following the variable.

        Either form is acceptable, but the two should not be mixed. Choose
        one convention to document module level variables and be consistent
        with it.

Todo:
    * For module TODOs
    * You have to also use ``sphinx.ext.todo`` extension

.. _Google Python Style Guide:
   http://google.github.io/styleguide/pyguide.html

"""
import sqlalchemy
import requests
import io
import os
import ntpath

from datetime import date, datetime, timedelta
from bs4 import BeautifulSoup
from sqlalchemy.exc import OperationalError

import pandas as pd

from gamblor import MIN_YEAR, NUM_TEAMS, STATS_CONN, AFL_TABLES_URL, ODDS_URL_DICT, SCORES_TABLE_COLUMNS, LADDER_TABLE_COLUMNS, ODDS_TABLE_COLUMNS, CHROME_USER_AGENT, ODDS_DIR

def round_before(search_date=date.today(),
                 conn_info=STATS_CONN):
    """Calculate the year and round that preceeded the input date.

    `PEP 484`_ type annotations are supported. If attribute, parameter, and
    return types are annotated according to `PEP 484`_, they do not need to be
    included in the docstring:

    Args:
        search_date (datetime): Datetime object denoting the date you wish to
            find the round immediately preceeding.
        conn_info (str): String containing the statistics database connection info.

    Returns:
        int, int: Year and round immediately precceding the input date.

    .. _PEP 484:
        https://www.python.org/dev/peps/pep-0484/

    """
    year = None
    rnd = None

    engine = sqlalchemy.create_engine(conn_info,
                                      echo=False)

    try:
        connection = engine.connect()
    except Exception as e:
        print(e)

    SQL_QUERY = """SELECT Scores.Year, Scores.Round, Scores.GameTime
                   FROM Scores
                """.format(search_date)

    try:
        match_df = pd.read_sql_query(SQL_QUERY,
                                     connection,
                                     parse_dates=["GameTime"])
        previous_matches_df = match_df[match_df["GameTime"] < pd.Timestamp(search_date)]

        if len(previous_matches_df) < 1:
            year = MIN_YEAR
            rnd = 1
        else:
            last_match_idx = previous_matches_df["GameTime"].values.argmax()
            last_match_df = previous_matches_df.iloc[last_match_idx, :]
            year = last_match_df["Year"]
            rnd = last_match_df["Round"]
    except OperationalError as error:
        year = MIN_YEAR
        rnd = 1

    return year, rnd

def next_round(year,
               rnd):
    next_year = None
    next_round = None

    if rnd < 23:
        next_year = year
        next_round = rnd + 1
    elif rnd == 23:
        next_year = year + 1
        next_round = 1

    return next_year, next_round

def next_match_date(conn_info=STATS_CONN,
                    year=MIN_YEAR,
                    rnd=1):
    """Calculate the year and round that preceeded the input date.

    `PEP 484`_ type annotations are supported. If attribute, parameter, and
    return types are annotated according to `PEP 484`_, they do not need to be
    included in the docstring:

    Args:
        search_date (datetime): Datetime object denoting the date you wish to
            find the round immediately preceeding.
        conn_info (str): String containing the statistics database connection info.

    Returns:
        int, int: Year and round immediately precceding the input date.

    .. _PEP 484:
        https://www.python.org/dev/peps/pep-0484/

    """
    next_match = None

    engine = sqlalchemy.create_engine(conn_info,
                                      echo=False)

    try:
        connection = engine.connect()
    except Exception as e:
        print(e)

    SQL_QUERY = """SELECT Scores.Year, Scores.Round, Scores.GameTime
                   FROM Scores
                   WHERE Scores.Year < {year} OR
                         (Scores.Round <= {rnd} AND Scores.Year = {year})
                """.format(year=year, rnd=rnd)

    try:
        match_df = pd.read_sql_query(SQL_QUERY,
                                     connection,
                                     parse_dates=["GameTime"])
        if rnd == 23:
            next_match = datetime.strptime("{}-03-01".format(year+1),
                                           "%Y-%m-%d").date()
        else:
            next_match = match_df["GameTime"].max().date() + timedelta(days=7)
    except OperationalError as error:
        next_match = datetime.strptime("{}-01-01".format(MIN_YEAR),
                                       "%Y-%m-%d").date()

    return next_match

def scrape_score_table(scrape_year=MIN_YEAR,
                       scrape_rnd=1):
    """Example function with types documented in the docstring.

    `PEP 484`_ type annotations are supported. If attribute, parameter, and
    return types are annotated according to `PEP 484`_, they do not need to be
    included in the docstring:

    Args:
        param1 (int): The first parameter.
        param2 (str): The second parameter.

    Returns:
        bool: The return value. True for success, False otherwise.

    .. _PEP 484:
        https://www.python.org/dev/peps/pep-0484/

    """
    web_site = AFL_TABLES_URL + str(scrape_year) + ".html"
    response = requests.get(web_site)
    soup = BeautifulSoup(response.content, "lxml")
    table = soup.find_all("table")
    df_list = pd.read_html(str(table))

    score_df = pd.DataFrame(columns=SCORES_TABLE_COLUMNS)
    is_finals = False
    rnd = None
    for df in df_list:
        if "Finals" in df.iloc[0, :].values:
            is_finals = True
            rnd = None
            break
        elif "Round" in df.iloc[0, :].values[0]:
            is_finals = False
            rnd = int(df.iloc[0, :].values[0].split()[-1])
            continue
        elif "Ladder" in df.iloc[0, :].values[0].split():
            continue
        elif "Ladder" in df.iloc[0, :].values[1].split():
            continue
        if isinstance(df.iloc[0, :].index, pd.core.index.MultiIndex):
            break
        elif rnd == scrape_rnd:
            if is_finals:
                match_type = "F"
            else:
                match_type = "IS"

            match_df = scrape_match(scrape_year,
                                    rnd,
                                    match_type,
                                    df)
            score_df = score_df.append(match_df,
                                       ignore_index=True,
                                       sort=False)

    score_df = score_df.drop_duplicates()

    return score_df

def scrape_match(year,
                 rnd,
                 match_type,
                 match_df):
    if len(match_df) == 1:
        score_df = scrape_bye(year,
                              rnd,
                              match_df)
    else:
        score_df = scrape_played_match(year,
                                       rnd,
                                       match_type,
                                       match_df)
    return score_df

def scrape_bye(year,
               rnd,
               match_df):
    score_df = pd.DataFrame({"Year": [year,],
                             "Round": [rnd,],
                             "Venue": [None,],
                             "GameType": ["IS",],
                             "GameTime": [None,],
                             "HomeTeam": [match_df.iloc[0, 0],],
                             "AwayTeam": ["Bye",],
                             "HomeFinalScore": [None,],
                             "AwayFinalScore": [None,],
                             "HomeQ1Goals": [None,],
                             "HomeQ1Points": [None,],
                             "HomeQ2Goals": [None,],
                             "HomeQ2Points": [None,],
                             "HomeQ3Goals": [None,],
                             "HomeQ3Points": [None,],
                             "HomeQ4Goals": [None,],
                             "HomeQ4Points": [None,],
                             "AwayQ1Goals": [None,],
                             "AwayQ1Points": [None,],
                             "AwayQ2Goals": [None,],
                             "AwayQ2Points": [None,],
                             "AwayQ3Goals": [None,],
                             "AwayQ3Points": [None,],
                             "AwayQ4Goals": [None,],
                             "AwayQ4Points": [None,]})

    return score_df

def scrape_played_match(year,
                        rnd,
                        match_type,
                        match_df):

    home_df = match_df.iloc[0, :]
    home_team = home_df[0]
    home_score_breakdown = home_df[1].split()
    home_q1g = int(home_score_breakdown[0].split(".")[0])
    home_q1p = int(home_score_breakdown[0].split(".")[1])
    home_q2g = int(home_score_breakdown[1].split(".")[0])
    home_q2p = int(home_score_breakdown[1].split(".")[1])
    home_q3g = int(home_score_breakdown[2].split(".")[0])
    home_q3p = int(home_score_breakdown[2].split(".")[1])
    home_q4g = int(home_score_breakdown[3].split(".")[0])
    home_q4p = int(home_score_breakdown[3].split(".")[1])
    home_final = int(home_df[2])

    away_df = match_df.iloc[1, :]
    away_team = away_df[0]
    away_score_breakdown = away_df[1].split()
    away_q1g = int(away_score_breakdown[0].split(".")[0])
    away_q1p = int(away_score_breakdown[0].split(".")[1])
    away_q2g = int(away_score_breakdown[1].split(".")[0])
    away_q2p = int(away_score_breakdown[1].split(".")[1])
    away_q3g = int(away_score_breakdown[2].split(".")[0])
    away_q3p = int(away_score_breakdown[2].split(".")[1])
    away_q4g = int(away_score_breakdown[3].split(".")[0])
    away_q4p = int(away_score_breakdown[3].split(".")[1])
    away_final = int(away_df[2])

    venue = home_df[3].split(":")[-1].strip()

    date_string = "{} {} {} {}".format(home_df[3].split(" ")[0],
                                       home_df[3].split(" ")[1],
                                       home_df[3].split(" ")[2],
                                       home_df[3].split(" ")[3])
    game_time = datetime.strptime(date_string, 
                                  "%a %d-%b-%Y %I:%M %p")

    score_df = pd.DataFrame({"Year": [int(year),],
                             "Round": [int(rnd),],
                             "Venue": [venue,],
                             "GameType": [match_type,],
                             "GameTime": [game_time,],
                             "HomeTeam": [home_team,],
                             "AwayTeam": [away_team,],
                             "HomeFinalScore": [home_final,],
                             "AwayFinalScore": [away_final,],
                             "HomeQ1Goals": [home_q1g,],
                             "HomeQ1Points": [home_q1p,],
                             "HomeQ2Goals": [home_q2g,],
                             "HomeQ2Points": [home_q2p,],
                             "HomeQ3Goals": [home_q3g,],
                             "HomeQ3Points": [home_q3p,],
                             "HomeQ4Goals": [home_q4g,],
                             "HomeQ4Points": [home_q4p,],
                             "AwayQ1Goals": [away_q1g,],
                             "AwayQ1Points": [away_q1p,],
                             "AwayQ2Goals": [away_q2g,],
                             "AwayQ2Points": [away_q2p,],
                             "AwayQ3Goals": [away_q3g,],
                             "AwayQ3Points": [away_q3p,],
                             "AwayQ4Goals": [away_q4g,],
                             "AwayQ4Points": [away_q4p,]})

    return score_df

def wierd_2015_round_14():
    # There was some weird shit in Round 14 2015
    score_df = pd.DataFrame({"Year": [2015,],
                             "Round": [14,],
                             "Venue": [None,],
                             "GameType": ["IS",],
                             "GameTime": [None,],
                             "HomeTeam": ["Adelaide",],
                             "AwayTeam": ["Bye",],
                             "HomeFinalScore": [None,],
                             "AwayFinalScore": [None,],
                             "HomeQ1Goals": [None,],
                             "HomeQ1Points": [None,],
                             "HomeQ2Goals": [None,],
                             "HomeQ2Points": [None,],
                             "HomeQ3Goals": [None,],
                             "HomeQ3Points": [None,],
                             "HomeQ4Goals": [None,],
                             "HomeQ4Points": [None,],
                             "AwayQ1Goals": [None,],
                             "AwayQ1Points": [None,],
                             "AwayQ2Goals": [None,],
                             "AwayQ2Points": [None,],
                             "AwayQ3Goals": [None,],
                             "AwayQ3Points": [None,],
                             "AwayQ4Goals": [None,],
                             "AwayQ4Points": [None,]})
    score_df = score_df.append({"Year": 2015,
                                "Round": 14,
                                "Venue": None,
                                "GameType": "IS",
                                "GameTime": None,
                                "HomeTeam": "Geelong",
                                "AwayTeam": "Bye",
                                "HomeFinalScore": None,
                                "AwayFinalScore": None,
                                "HomeQ1Goals": None,
                                "HomeQ1Points": None,
                                "HomeQ2Goals": None,
                                "HomeQ2Points": None,
                                "HomeQ3Goals": None,
                                "HomeQ3Points": None,
                                "HomeQ4Goals": None,
                                "HomeQ4Points": None,
                                "AwayQ1Goals": None,
                                "AwayQ1Points": None,
                                "AwayQ2Goals": None,
                                "AwayQ2Points": None,
                                "AwayQ3Goals": None,
                                "AwayQ3Points": None,
                                "AwayQ4Goals": None,
                                "AwayQ4Points": None},
                               ignore_index=True)

    return score_df

def scrape_ladder_table(scrape_year=MIN_YEAR,
                        scrape_rnd=1):
    """Example function with types documented in the docstring.

    `PEP 484`_ type annotations are supported. If attribute, parameter, and
    return types are annotated according to `PEP 484`_, they do not need to be
    included in the docstring:

    Args:
        param1 (int): The first parameter.
        param2 (str): The second parameter.

    Returns:
        bool: The return value. True for success, False otherwise.

    .. _PEP 484:
        https://www.python.org/dev/peps/pep-0484/

    """
    web_site = AFL_TABLES_URL + str(scrape_year) + ".html"
    response = requests.get(web_site)
    soup = BeautifulSoup(response.content, "lxml")
    table = soup.find_all("table")
    df_list = pd.read_html(str(table))

    ladder_df = pd.DataFrame(columns=LADDER_TABLE_COLUMNS)
    is_finals = False
    rnd = None
    for df in df_list:
        if "Finals" in df.iloc[0, :].values:
            is_finals = True
            rnd = None
            break
        elif "Round" in df.iloc[0, :].values[0]:
            is_finals = False
            is_ladder = False
            rnd = int(df.iloc[0, :].values[0].split()[-1])
            continue
        elif "Ladder" in df.iloc[0, :].values[0].split() and rnd == scrape_rnd:
            round_df = scrape_round_ladder(scrape_year,
                                           scrape_rnd,
                                           df)
            ladder_df = ladder_df.append(round_df,
                                         ignore_index=True,
                                         sort=False)
        elif "Ladder" in df.iloc[0, :].values[1].split():
            continue
        if isinstance(df.iloc[0, :].index, pd.core.index.MultiIndex):
            break
        elif rnd == scrape_rnd and is_ladder:
            pass

    ladder_df = ladder_df.drop_duplicates()

    return ladder_df

def scrape_round_ladder(year,
                        rnd,
                        round_df):
    ladder_df = pd.DataFrame(columns=LADDER_TABLE_COLUMNS)
    for t in range(NUM_TEAMS):
        team = round_df.iloc[t+1, 0]
        num_games = round_df.iloc[t+1, 1]
        points = round_df.iloc[t+1, 2]
        percentage = round_df.iloc[t+1, 3]
        team_df = pd.DataFrame({"Year": [year,],
                                  "Round": [rnd,],
                                  "Team": [team,],
                                  "GamesPlayed": [int(num_games),],
                                  "Points": [int(points),],
                                  "Percentage": [float(percentage),]})
        ladder_df = ladder_df.append(team_df,
                                         ignore_index=True,
                                         sort=False)

    return ladder_df

def scrape_odds_table(scrape_year=MIN_YEAR,
                      scrape_rnd=1):
    """Example function with types documented in the docstring.

    `PEP 484`_ type annotations are supported. If attribute, parameter, and
    return types are annotated according to `PEP 484`_, they do not need to be
    included in the docstring:

    Args:
        param1 (int): The first parameter.
        param2 (str): The second parameter.

    Returns:
        bool: The return value. True for success, False otherwise.

    .. _PEP 484:
        https://www.python.org/dev/peps/pep-0484/

    """
    historical_df = pd.DataFrame(columns=ODDS_TABLE_COLUMNS)
    url = ODDS_URL_DICT[scrape_year]
    filename = ntpath.basename(url)
    filepath = os.path.join(ODDS_DIR,
                            filename)
    if not os.path.isfile(filepath):
        response = requests.get(url, headers=CHROME_USER_AGENT)
        with open(filepath, "wb") as excel_file:
            excel_file.write(response.content)

    header_row = 0
    if filename == "AFL-Data-Dump-2017.xlsx":
        header_row = 3
    odds_df = pd.read_excel(filepath,
                            header=header_row)
    odds_df.columns = [x.lower() for x in odds_df.columns]
        
    if filename == "AFL-2011-2016.xlsx":
        odds_df.rename(columns={"path": "paths"}, 
                       inplace=True)

    odds_df = odds_df[(odds_df["inplay"] == "N") & \
                      (odds_df["event_name"] == "Match Odds")]
    odds_df = odds_df[odds_df["paths"].str.contains("AFL")]
    odds_df = odds_df[~odds_df["paths"].str.contains("WAFL", regex=False)]
    odds_df = odds_df[~odds_df["selection_name"].str.contains("(W)", regex=False)]
        
    if filename == "AFL-2011-2016.xlsx":
        odds_df["Year"] = odds_df["paths"].str.split("/", expand=True)[0].str.split(" ", expand=True)[1].astype(int)
    else:
        odds_df["Year"] = odds_df["sett_date"].dt.year.astype(int)
    
    odds_df["HomeTeam"] = odds_df["parent_event_name"].str.split("v", expand=True)[0]
    odds_df["HomeTeam"] = odds_df["HomeTeam"].str.strip()
    odds_df["AwayTeam"] = odds_df["parent_event_name"].str.split("v", expand=True)[1]
    odds_df["AwayTeam"] = odds_df["AwayTeam"].str.strip()

    # Fix wierd names
    wierd_name_dict = {"Port Adelaide Power": "Port Adelaide",
                       "Adelaide Crows": "Adelaide",
                       "Melbourne Demons": "Melbourne",
                       "Gold Coast Suns": "Gold Coast",
                       "Geelong Cats": "Geelong",
                       "Sydney Swans": "Sydney",
                       "GWS Giants": "Greater Western Sydney",
                       "GWS": "Greater Western Sydney",
                       "West Coast Eagles": "West Coast",
                       "Brisbane": "Brisbane Lions",
                      }
    for wierd_name, fixed_name in wierd_name_dict.items():
        odds_df.loc[odds_df["HomeTeam"] == wierd_name, "HomeTeam"] = fixed_name
        odds_df.loc[odds_df["AwayTeam"] == wierd_name, "AwayTeam"] = fixed_name

    try:
        engine = sqlalchemy.create_engine(STATS_CONN,
                                          echo=False)
        connection = engine.connect()
    except Exception as e:
        print(e)

    SQL_QUERY = """ SELECT Scores.MatchID, Scores.Year, Scores.Round, Scores.GameTime, Scores.HomeTeam, Scores.AwayTeam
                    FROM Scores
                    WHERE Scores.Round = {0}
                    AND Scores.Year = {1}
                """.format(scrape_rnd,
                           scrape_year)

    scores_df = pd.read_sql_query(SQL_QUERY,
                                  connection,
                                  parse_dates=["GameTime"])

    scores_df = scores_df.set_index(["Year", "HomeTeam", "AwayTeam"],
                                    drop=True)

    odds_df = odds_df.join(scores_df,
                           on=["Year", "HomeTeam", "AwayTeam"],
                           how="inner")

    url_df = pd.DataFrame({"MatchID": odds_df["MatchID"].values,
                           "Year": odds_df["Year"].values,
                           "Round": odds_df["Round"].values,
                           "GameTime": odds_df["GameTime"].values,
                           "Team": odds_df["selection_name"],
                           "Odds": odds_df["wap"]})

    for wierd_name, fixed_name in wierd_name_dict.items():
        url_df.loc[url_df["Team"] == wierd_name, "Team"] = fixed_name

    historical_df = historical_df.append(url_df,
                                         ignore_index=True,
                                         sort=False)

    historical_df = historical_df.drop_duplicates(subset="Team",
                                                  keep="first")


    return historical_df
