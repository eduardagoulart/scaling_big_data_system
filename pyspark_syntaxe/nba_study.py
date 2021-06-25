import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as fn


spark = SparkSession.builder.appName("PySpark").getOrCreate()
games = spark.read.csv("datasets/nba/games.csv", header=True)
games_details = spark.read.csv("datasets/nba/games_details.csv", header=True)
players = spark.read.csv("datasets/nba/players.csv", header=True)
ranking = spark.read.csv("datasets/nba/ranking.csv", header=True)
teams = spark.read.csv("datasets/nba/teams.csv", header=True)


def check_shema(df):
    print(df.printSchema())


def get_team_with_best_score_during_season():
    """
    Problem: I want to know which was the team
    that score more points in a game at home and
    away
    :return:
        String (team name with best score at home)
        String (team name with best score as visitor)
    """

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

    team_name = teams.select(["TEAM_ID", "ABBREVIATION", "NICKNAME", "CITY"])
    team_name_home = team_name.selectExpr(
        "TEAM_ID AS HOME_TEAM_ID", "ABBREVIATION", "NICKNAME", "CITY"
    )
    team_name_away = team_name.selectExpr(
        "TEAM_ID AS VISITOR_TEAM_ID", "ABBREVIATION", "NICKNAME", "CITY"
    )

    home_team_best_score = games.select(
        ["GAME_ID", "HOME_TEAM_ID", "VISITOR_TEAM_ID", "PTS_home", "PTS_away"]
    ).where(games["PTS_home"] == max_score_home)

    away_team_best_score = games.select(
        ["GAME_ID", "HOME_TEAM_ID", "VISITOR_TEAM_ID", "PTS_home", "PTS_away"]
    ).where(games["PTS_away"] == max_score_away)

    home_team_output = home_team_best_score.join(
        team_name_home, on=["HOME_TEAM_ID"], how="left"
    )
    home_team_output = home_team_output.join(
        team_name_away, on=["VISITOR_TEAM_ID"], how="left"
    )

    away_team_output = away_team_best_score.join(
        team_name_home, on=["HOME_TEAM_ID"], how="left"
    )
    away_team_output = away_team_output.join(
        team_name_away, on=["VISITOR_TEAM_ID"], how="left"
    )

    return home_team_output, away_team_output


def details_about_team(winner, place):
    """
    I want to know details about a team,
    giving a DF with data about a game, I want to know who were the
    player on it, and which one scored more points
    :param
        winner: PySpark DF that contains the game details
        place: String ("HOME" or "AWAY")
    :return: PySpark DF with the game MVP
    """
    games_details_by_place = games_details.selectExpr(
        f"TEAM_ID AS {place}_TEAM_ID",
        "TEAM_ABBREVIATION",
        "PLAYER_NAME",
        "PTS",
        "GAME_ID",
    )

    df = winner.join(
        games_details_by_place, on=["GAME_ID", f"{place}_TEAM_ID"], how="left"
    )
    float_points = df.select(df["PTS"].cast("float"))
    max_score = (
        float_points.select(fn.max("PTS").alias("MAX")).limit(1).collect()[0].MAX
    )
    mvp_game = df.select(
        ["GAME_ID", f"{place}_TEAM_ID", "PLAYER_NAME", "PTS", "TEAM_ABBREVIATION"]
    ).where(df["PTS"] == max_score)
    return mvp_game


if "__main__" == __name__:
    home_team_wins, away_team_wins = get_team_with_best_score_during_season()
    mvp_at_home_game = details_about_team(home_team_wins, place="HOME")
