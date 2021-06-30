from scaling_big_data_system.pyspark_syntaxe import nba_study

expected_columns = [
    "VISITOR_TEAM_ID",
    "HOME_TEAM_ID",
    "GAME_ID",
    "PTS_home",
    "PTS_VISITOR",
    "ABBREVIATION",
    "NICKNAME",
    "CITY",
    "ABBREVIATION",
    "NICKNAME",
    "CITY",
    "winner",
]
expected_columns.sort()


def test_get_team_with_best_score_during_season_home_columns():
    output = nba_study.get_team_with_best_score_during_season("HOME")
    output_columns = output.columns
    output_columns.sort()

    assert expected_columns == output_columns


def test_get_team_with_best_score_during_season_home_result():
    output = nba_study.get_team_with_best_score_during_season("HOME")
    winner_name = output.select("winner").collect()[0].winner

    assert "Denver Nuggets" == winner_name


def test_get_team_with_best_score_during_season_visitor_columns():
    output = nba_study.get_team_with_best_score_during_season("VISITOR")
    output_columns = output.columns
    output_columns.sort()

    assert expected_columns == output_columns


def test_get_team_with_best_score_during_season_visitor_result():
    output = nba_study.get_team_with_best_score_during_season("VISITOR")
    winner_name = output.select("winner").collect()[0].winner

    assert "Chicago Bulls" == winner_name
