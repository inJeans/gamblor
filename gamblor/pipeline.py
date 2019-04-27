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
import os
import datetime
import luigi
import argparse

from datetime import datetime, date
from luigi.contrib import sqla

import pandas as pd

from gamblor.data_collection import scrape_score_table, scrape_ladder_table, scrape_odds_table
from gamblor.data_collection import round_before, next_round, next_match_date
from gamblor import SCORE_DIR, LADDER_DIR, ODDS_DIR, MIN_YEAR, STATS_CONN, LUIGI_LADDER_TABLE_COLUMNS, LUIGI_SCORES_TABLE_COLUMNS, LUIGI_ODDS_TABLE_COLUMNS

START_DATE = str(MIN_YEAR) + "-01-01"
END_DATE = date.today().strftime("%Y-%m-%d")
END_DATE = "2018-12-31"

class CreateScoresFile(luigi.Task):
    """The summary line for a class docstring should fit on one line.

    If the class has public attributes, they may be documented here
    in an ``Attributes`` section and follow the same formatting as a
    function's ``Args`` section. Alternatively, attributes may be documented
    inline with the attribute's declaration (see __init__ method below).

    Properties created with the ``@property`` decorator should be documented
    in the property's getter method.

    Attributes:
        attr1 (str): Description of `attr1`.
        attr2 (:obj:`int`, optional): Description of `attr2`.

    """
    year = luigi.IntParameter(default=MIN_YEAR)
    rnd = luigi.IntParameter(default=1)

    def run(self):
        """Class methods are similar to regular functions.

        Note:
            Do not include the `self` parameter in the ``Args`` section.

        Args:
            param1: The first parameter.
            param2: The second parameter.

        Returns:
            True if successful, False otherwise.

        """
        scores_df = scrape_score_table(scrape_year=self.year,
                                       scrape_rnd=self.rnd)

        scores_df.to_pickle(self.output().path)

    def output(self):
        """Class methods are similar to regular functions.

        Note:
            Do not include the `self` parameter in the ``Args`` section.

        Args:
            param1: The first parameter.
            param2: The second parameter.

        Returns:
            True if successful, False otherwise.

        """
        ouput_path = os.path.join(SCORE_DIR,
                                  "{}-{}.pkl".format(self.year, self.rnd))
        return luigi.LocalTarget(ouput_path)

class CreateLadderFile(luigi.Task):
    """The summary line for a class docstring should fit on one line.

    If the class has public attributes, they may be documented here
    in an ``Attributes`` section and follow the same formatting as a
    function's ``Args`` section. Alternatively, attributes may be documented
    inline with the attribute's declaration (see __init__ method below).

    Properties created with the ``@property`` decorator should be documented
    in the property's getter method.

    Attributes:
        attr1 (str): Description of `attr1`.
        attr2 (:obj:`int`, optional): Description of `attr2`.

    """
    year = luigi.IntParameter(default=MIN_YEAR)
    rnd = luigi.IntParameter(default=1)

    def run(self):
        """Class methods are similar to regular functions.

        Note:
            Do not include the `self` parameter in the ``Args`` section.

        Args:
            param1: The first parameter.
            param2: The second parameter.

        Returns:
            True if successful, False otherwise.

        """
        scores_df = scrape_ladder_table(scrape_year=self.year,
                                        scrape_rnd=self.rnd)

        scores_df.to_pickle(self.output().path)

    def output(self):
        """Class methods are similar to regular functions.

        Note:
            Do not include the `self` parameter in the ``Args`` section.

        Args:
            param1: The first parameter.
            param2: The second parameter.

        Returns:
            True if successful, False otherwise.

        """
        ouput_path = os.path.join(LADDER_DIR,
                                  "{}-{}.pkl".format(self.year, self.rnd))
        return luigi.LocalTarget(ouput_path)

class CreateOddsFile(luigi.Task):
    """The summary line for a class docstring should fit on one line.

    If the class has public attributes, they may be documented here
    in an ``Attributes`` section and follow the same formatting as a
    function's ``Args`` section. Alternatively, attributes may be documented
    inline with the attribute's declaration (see __init__ method below).

    Properties created with the ``@property`` decorator should be documented
    in the property's getter method.

    Attributes:
        attr1 (str): Description of `attr1`.
        attr2 (:obj:`int`, optional): Description of `attr2`.

    """
    year = luigi.IntParameter(default=MIN_YEAR)
    rnd = luigi.IntParameter(default=1)

    def requires(self):
        return WriteScoresToDB(self.year, self.rnd)

    def run(self):
        """Class methods are similar to regular functions.

        Note:
            Do not include the `self` parameter in the ``Args`` section.

        Args:
            param1: The first parameter.
            param2: The second parameter.

        Returns:
            True if successful, False otherwise.

        """
        odds_df = scrape_odds_table(scrape_year=self.year,
                                    scrape_rnd=self.rnd)

        odds_df.to_pickle(self.output().path)

    def output(self):
        """Class methods are similar to regular functions.

        Note:
            Do not include the `self` parameter in the ``Args`` section.

        Args:
            param1: The first parameter.
            param2: The second parameter.

        Returns:
            True if successful, False otherwise.

        """
        ouput_path = os.path.join(ODDS_DIR,
                                  "{}-{}.pkl".format(self.year, self.rnd))
        return luigi.LocalTarget(ouput_path)

class WriteScoresToDB(sqla.CopyToTable):
    year = luigi.IntParameter(default=MIN_YEAR)
    rnd = luigi.IntParameter(default=1)
    
    columns = LUIGI_SCORES_TABLE_COLUMNS
    connection_string = STATS_CONN
    table = "Scores"  # name of the table to store data

    def requires(self):
        return CreateScoresFile(self.year, self.rnd)

    def rows(self):
        scores_df = pd.read_pickle(self.input().path)
        scores_df["GameTime"] = scores_df["GameTime"].dt.strftime("%Y-%m-%d %H:%M")
        for m, match in scores_df.iterrows():
            yield (None,
                   match["Year"],
                   match["Round"],
                   match["GameType"],
                   match["Venue"],
                   match["GameTime"],
                   match["HomeTeam"],
                   match["AwayTeam"],
                   match["HomeFinalScore"],
                   match["AwayFinalScore"],
                   match["HomeQ1Goals"], match["HomeQ1Points"],
                   match["HomeQ2Goals"], match["HomeQ2Points"],
                   match["HomeQ3Goals"], match["HomeQ3Points"],
                   match["HomeQ4Goals"], match["HomeQ4Points"],
                   match["AwayQ1Goals"], match["AwayQ1Points"],
                   match["AwayQ2Goals"], match["AwayQ2Points"],
                   match["AwayQ3Goals"], match["AwayQ3Points"],
                   match["AwayQ4Goals"], match["AwayQ4Points"],
                   )

class WriteLadderToDB(sqla.CopyToTable):
    year = luigi.IntParameter(default=MIN_YEAR)
    rnd = luigi.IntParameter(default=1)

    columns = LUIGI_LADDER_TABLE_COLUMNS
    connection_string = STATS_CONN
    table = "Ladder"  # name of the table to store data

    def requires(self):
        return CreateLadderFile(self.year, self.rnd)

    def rows(self):
        ladder_df = pd.read_pickle(self.input().path)
        for _, team in ladder_df.iterrows():
            yield (team["Year"],
                   team["Round"],
                   team["Team"],
                   team["GamesPlayed"],
                   team["Points"],
                   team["Percentage"])

class WriteOddsToDB(sqla.CopyToTable):
    year = luigi.IntParameter(default=MIN_YEAR)
    rnd = luigi.IntParameter(default=1)
    
    columns = LUIGI_ODDS_TABLE_COLUMNS
    connection_string = STATS_CONN
    table = "Odds"  # name of the table to store data

    def requires(self):
        return CreateOddsFile(self.year, self.rnd)

    def rows(self):
        odds_df = pd.read_pickle(self.input().path)
        odds_df["GameTime"] = odds_df["GameTime"].dt.strftime("%Y-%m-%d %H:%M")
        for o, odd in odds_df.iterrows():
            yield (odd["MatchID"],
                   odd["Year"],
                   odd["Round"],
                   odd["GameTime"],
                   odd["Team"],
                   odd["Odds"])

def main(args):
    match_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    # end_date = datetime.strptime("2014-04-01", "%Y-%m-%d").date()

    year, rnd = round_before(search_date=match_date,
                             conn_info=STATS_CONN)

    while match_date < end_date:
        luigi.build([
                     WriteLadderToDB(year, rnd),
                     WriteScoresToDB(year, rnd),
                     WriteOddsToDB(year, rnd)
                     ],
                     local_scheduler=True)

        year, rnd = next_round(year,
                               rnd)
        match_date = next_match_date(STATS_CONN,
                                     year,
                                     rnd)

        print(match_date, year, rnd)

def pipeline_cli():
    parser = argparse.ArgumentParser(description="Gamblor prediction pipeline.")
    parser.add_argument("--start_date", "-s",
                        type=str,
                        default=START_DATE,
                        help="Date to start collecting data from.")
    parser.add_argument('--end_date', "-e",
                        type=str,
                        default=END_DATE,
                        help="Date to collect data up to.")

    args = parser.parse_args()

    main(args)

if __name__ == "__main__":
    pipeline_cli()
