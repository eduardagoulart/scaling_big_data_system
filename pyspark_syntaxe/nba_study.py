import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("PySpark").getOrCreate()
games = spark.read.csv("datasets/nba/games.csv", header=True)
games_details = spark.read.csv("datasets/nba/games_details.csv", header=True)
players = spark.read.csv("datasets/nba/players.csv", header=True)
ranking = spark.read.csv("datasets/nba/ranking.csv", header=True)
teams = spark.read.csv("datasets/nba/teams.csv", header=True)


def check_shema(df):
    print(df.printSchema())


def get_team_with_best_score_during_season(year):
    """
    Problem: giving a season year, I want to know which team has scored more
    point during that year
    :param year: Integer
    :return: String (team name)
    """
    games = spark.read.csv("datasets/nba/games.csv", header=True)
    print(games.show())

    pandas_games = games.toPandas()
    pandas_games["PTS_home"] = pandas_games["PTS_home"].fillna(0.0)
    pandas_games["PTS_home"] = pandas_games["PTS_home"].astype(float)
    # pandas_games = pandas_games.sort_values(by=["PTS_home"])
    print(max(pandas_games["PTS_home"]))

    pandas_games["PTS_away"] = pandas_games["PTS_away"].fillna(0.0)
    pandas_games["PTS_away"] = pandas_games["PTS_away"].astype(float)
    pandas_games = pandas_games.sort_values(by=["PTS_away"])
    print(max(pandas_games["PTS_away"]))
    print("----------- " * 10)

    float_value_home = games.select(games["PTS_home"].cast("float"))
    float_value_away = games.select(games["PTS_away"].cast("float"))
    max_score_home = (
        float_value_home.select(fn.max("PTS_home").alias("MAX"))
        .limit(1)
        .collect()[0]
        .MAX
    )
    max_score_away = (
        float_value_away.select(fn.max("PTS_away").alias("MAX"))
        .limit(1)
        .collect()[0]
        .MAX
    )

    home_team_best_score = games.select(
        ["GAME_ID", "HOME_TEAM_ID", "VISITOR_TEAM_ID", "PTS_home", "PTS_away"]
    ).where(games["PTS_home"] == max_score_home)

    print(home_team_best_score.show())
    print(max_score_away)


if "__main__" == __name__:
    get_team_with_best_score_during_season(year=2017)
