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


def test_details_about_team_visitor_result():
    team_winner = nba_study.get_team_with_best_score_during_season("VISITOR")
    output = nba_study.details_about_team(team_winner, "VISITOR")
    mvp = output.select("PLAYER_NAME").collect()[0].PLAYER_NAME
    team_abbreviation = (
        output.select("TEAM_ABBREVIATION").collect()[0].TEAM_ABBREVIATION
    )

    assert "Zach LaVine" == mvp
    assert team_abbreviation == "CHI"


def test_details_about_team_home_result():
    team_winner = nba_study.get_team_with_best_score_during_season("HOME")
    output = nba_study.details_about_team(team_winner, "HOME")
    mvp = output.select("PLAYER_NAME").collect()[0].PLAYER_NAME
    team_abbreviation = (
        output.select("TEAM_ABBREVIATION").collect()[0].TEAM_ABBREVIATION
    )

    assert "Carmelo Anthony" == mvp
    assert team_abbreviation == "DEN"
