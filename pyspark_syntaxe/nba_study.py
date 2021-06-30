from pyspark.sql import SparkSession
import pyspark.sql.functions as fn


spark = SparkSession.builder.appName("PySpark").getOrCreate()
games = spark.read.csv("pyspark_syntaxe/datasets/nba/games.csv", header=True)
games_details = spark.read.csv(
    "pyspark_syntaxe/datasets/nba/games_details.csv", header=True
)
players = spark.read.csv(
    "pyspark_syntaxe/datasets/nba/players.csv", header=True
)
ranking = spark.read.csv(
    "pyspark_syntaxe/datasets/nba/ranking.csv", header=True
)
teams = spark.read.csv("pyspark_syntaxe/datasets/nba/teams.csv", header=True)


def check_shema(df):
    print(df.printSchema())


def get_team_with_best_score_during_season(place):
    """
    Problem: I want to know which was the team
    that score more points in a game at home and
    away
    :param place: String ("HOME" or "VISITOR")
    :return:
        String (team name with best score at home)
        String (team name with best score as visitor)
    """
    # print(games.show())
    games_info = games.selectExpr(
        "GAME_DATE_EST",
        "GAME_ID",
        "GAME_STATUS_TEXT",
        "HOME_TEAM_ID",
        "VISITOR_TEAM_ID",
        "SEASON",
        "TEAM_ID_home",
        "PTS_home",
        "FG_PCT_home",
        "FT_PCT_home",
        "FG3_PCT_home",
        "AST_home",
        "REB_home",
        "TEAM_ID_away AS TEAM_ID_VISITOR",
        "PTS_away AS PTS_VISITOR",
        "FG_PCT_away AS FG_PCT_VISITOR",
        "FT_PCT_away AS FT_PCT_VISITOR",
        "FG3_PCT_away AS FG3_PCT_VISITOR",
        "AST_away AS AST_VISITOR",
        "REB_away AS REB_VISITOR",
        "HOME_TEAM_WINS",
    )

    float_value = games_info.select(games_info[f"PTS_{place}"].cast("float"))

    max_score = (
        float_value.select(fn.max(f"PTS_{place}").alias("MAX"))
        .limit(1)
        .collect()[0]
        .MAX
    )

    team_name = teams.selectExpr(
        f"TEAM_ID AS {place}_TEAM_ID", "ABBREVIATION", "NICKNAME", "CITY"
    )

    team_best_score = games_info.select(
        [
            "GAME_ID",
            "HOME_TEAM_ID",
            "VISITOR_TEAM_ID",
            "PTS_home",
            "PTS_VISITOR",
        ]
    ).where(games_info[f"PTS_{place}"] == max_score)

    team_output = team_best_score.join(
        team_name, on=[f"{place}_TEAM_ID"], how="left"
    )
    winner_nickname = team_output.select("NICKNAME").collect()[0].NICKNAME
    winner_city = team_output.select("CITY").collect()[0].CITY
    winner_name = f"{winner_city} {winner_nickname}"

    if place == "VISITOR":
        other = "HOME"
    else:
        other = "VISITOR"

    other_team_name = teams.selectExpr(
        f"TEAM_ID AS {other}_TEAM_ID", "ABBREVIATION", "NICKNAME", "CITY"
    )

    team_output = team_output.join(
        other_team_name, on=[f"{other}_TEAM_ID"], how="left"
    )
    team_output = team_output.withColumn("winner", fn.lit(winner_name))
    print(team_output.show())

    return team_output


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
        float_points.select(fn.max("PTS").alias("MAX"))
        .limit(1)
        .collect()[0]
        .MAX
    )
    mvp_game = df.select(
        [
            "GAME_ID",
            f"{place}_TEAM_ID",
            "PLAYER_NAME",
            "PTS",
            "TEAM_ABBREVIATION",
        ]
    ).where(df["PTS"] == max_score)
    return mvp_game


if "__main__" == __name__:
    # team_data = get_team_with_best_score_during_season(
    #     place="HOME"
    # )
    team_data = get_team_with_best_score_during_season(place="VISITOR")
    # mvp_at_home_game = details_about_team(home_team_wins, place="HOME")
