import numpy as np
import pandas as pd

# get unique dictionary of team to franchise IDs to merge into following dataframes
teams = pd.read_csv("project_data\LahmanData2023\core\Teams.csv")

franch_IDs = []
for team in teams["teamID"].unique():
    franch_IDs.append(teams[teams["teamID"] == team]["franchID"].iloc(0)[0])

franchises = pd.DataFrame(
    list(zip(teams["teamID"].unique(), franch_IDs)), columns=["teamID", "franchID"]
)

# case sensitive fix for some differences in formatting
warbat = pd.read_csv("project_data\war_daily_bat.txt")
warbat = warbat[warbat["year_ID"] > 1984]
for a in warbat["team_ID"].unique():
    if not a in list(franchises["teamID"]):
        if a in list(franchises["franchID"]):
            franchises.loc[len(franchises.index)] = [a, a]
        else:
            franchises.loc[len(franchises.index)] = ["TBR", "TBD"]


salary = pd.read_csv("project_data\LahmanData2023\contrib\Salaries.csv").merge(
    franchises, on="teamID"
)

# merging franchise ID data onto WAR data from baseballreference
war_batting = warbat.merge(franchises, left_on="team_ID", right_on="teamID")
war_pitching = pd.read_csv("project_data\war_daily_pitch.txt").merge(
    franchises, left_on="team_ID", right_on="teamID"
)

# merging pitching and batting WAR dataframe into combined "players" dataframe
players = war_batting.merge(
    war_pitching, how="left", on=["player_ID", "year_ID", "franchID"]
)


# creating a column of what number season the player is in. 1 for their first season, and so on.
season = []
first_season = {}

for i in range(len(players)):
    ID = players["player_ID"][i]
    if ID in first_season.keys():
        s = players["year_ID"][i] - first_season[ID] + 1
    else:
        first_year = min(players[players["player_ID"] == ID]["year_ID"])
        first_season[ID] = first_year
        s = players["year_ID"][i] - first_year + 1
    season.append(s)

players["season"] = season

# eliminating player seasons outside the range of available salary data
players = players[players["year_ID"] > 1984]
players = players[players["year_ID"] != 1994]
players = players[players["year_ID"] < 2020]
players.reset_index(inplace=True)

# summing pitching and batting WAR to create WAR_total column
WAR_total = []
for i in range(len(players)):
    x = players["WAR_x"][i]
    y = players["WAR_y"][i]

    x_true = pd.isna(players["WAR_x"][i])
    y_true = pd.isna(players["WAR_y"][i])

    if not (x_true or y_true):
        war = x + y
    elif x_true and not y_true:
        war = y
    elif y_true and not x_true:
        war = x
    else:
        war = 0

    WAR_total.append(war)

players["WAR_total"] = WAR_total

players_salary = players[
    (~players["salary_x"].isnull()) & (players["salary_x"] > 10000)
]

### adding estimated salaries for players with missing salary data

# dropping players with multiple stints to keep just one stint
# and use the non-NaN salary for that player
stinters = (
    players[players["stint_ID_x"] > 1]
    .loc[:, ["player_ID", "year_ID"]]
    .reset_index(drop=True)
)

drop_idx = []
for i in range(len(stinters)):
    idx = players[
        (players["player_ID"] == stinters.iloc[i][0])
        & (players["year_ID"] == stinters.iloc[i][1])
        & (players["salary_x"].isnull())
    ].index.tolist()
    for a in idx:
        if a not in drop_idx:
            drop_idx.append(a)

drop_idx.sort()

players_dropped = players.iloc[drop_idx].reset_index(drop=True)
players = players.drop(index=drop_idx).reset_index(drop=True)


avg_player_season_salary = (
    players_salary.loc[:, ["year_ID", "season", "salary_x"]]
    .groupby(by=["year_ID", "season"])
    .agg([np.mean, len])
    .reset_index()
)
# calculating salary minimum
# by checking the lowest salary of players with more than 130 games that season
yearly_minimum_salary = {}
for year in players["year_ID"].unique():
    yearly_minimum_salary[year] = min(
        players[
            (players["year_ID"] == year)
            & (players["season"] < 4)
            & (players["G_x"] > 130)
        ]["salary_x"]
    )

# new salary column "estimated_salary"
# for players without salary in year 1-3, set salary to minimum * %games
# for players without salary in year 4+, set salary to avg salary for that year/season
# set estimated_salary null for all players with salary_x
est_salaries = []
for i in range(len(players)):
    year = players["year_ID"][i]

    if np.isnan(players["salary_x"][i]) or players["salary_x"][i] < 10000:
        if players["season"][i] < 4:
            if players["pitcher"][i] == "Y":
                if players["G_y"][i] > 30:
                    est = yearly_minimum_salary[year]
                else:
                    est = yearly_minimum_salary[year] * (players["G_y"][i] * 3) / 162
            else:
                if players["G_x"][i] > 100:
                    est = yearly_minimum_salary[year]
                else:
                    est = yearly_minimum_salary[year] * (players["G_x"][i]) / 162
        else:
            try:
                est = float(
                    avg_player_season_salary[
                        (avg_player_season_salary["year_ID"] == year)
                        & (avg_player_season_salary["season"] == players["season"][i])
                    ]["salary_x"]["mean"]
                )
            except:
                est = yearly_minimum_salary[year]

    else:
        est = np.NaN
    est_salaries.append(est)

players["estimated_salary"] = est_salaries

# new salary column "aggregated_salary" = sum(max(salary_x and estimated_salary))
agg_sal = []
for i in range(len(players)):
    agg_sal.append(np.nanmax([players["salary_x"][i], players["estimated_salary"][i]]))

players["aggregated_salary"] = agg_sal
players["WAR_per_salary"] = (players["WAR_total"] * 1000000) / players[
    "aggregated_salary"
]

players_plus = players._append(players_dropped)

# creating teams as sumations of all players on that team as a new dataframe
teams = (
    players_plus.loc[
        :, ["year_ID", "franchID", "salary_x", "WAR_total", "aggregated_salary"]
    ]
    .groupby(["year_ID", "franchID"])
    .agg(sum)
    .reset_index()
    .merge(teams, left_on=["year_ID", "franchID"], right_on=["yearID", "franchID"])
)


teams_salary = (
    players_salary.loc[:, ["year_ID", "franchID", "salary_x", "WAR_total"]]
    .groupby(["year_ID", "franchID"])
    .agg(sum)
    .reset_index()
    .merge(teams, left_on=["year_ID", "franchID"], right_on=["yearID", "franchID"])
)

# team_salary_bref_batting = war_batting.groupby(['year_ID', 'franchID']).agg(
#     sum).reset_index().loc[:, ['year_ID', 'franchID', 'salary', 'WAR']]
# teams = team_salary_bref_batting.merge(
#     teams, left_on=['year_ID', 'franchID'], right_on=['yearID', 'franchID'])

# adding playoff stat column
playoffs = []
for i in range(len(teams)):
    if teams["WCWin"][i] == "Y" or teams["DivWin"][i] == "Y":
        playoffs.append("Y")
    else:
        playoffs.append("N")

teams["playoffs"] = playoffs
teams_salary["playoffs"] = playoffs


player_playoffs = []
for i in range(len(players)):
    player_playoffs.append(
        teams[
            (teams["franchID"] == players["franchID"][i])
            & (teams["yearID"] == players["year_ID"][i])
        ].reset_index()["playoffs"][0]
    )

players["playoffs"] = player_playoffs

# incorporating average and std statistics for salary into dataset
# as well as aggregated statistings and plus statistics

players_average = (
    players.loc[:, ["year_ID", "salary_x", "aggregated_salary"]]
    .groupby("year_ID")
    .agg(["mean", np.std, np.median])
    .reset_index()
)

teams_average = (
    teams.loc[:, ["year_ID", "salary_x", "aggregated_salary"]]
    .groupby("year_ID")
    .agg(["mean", np.std, np.median])
    .reset_index()
)

pct_teams_average = []
agg_sal_plus = []

for i in range(len(teams)):
    pct_teams_average.append(
        teams["salary_x"][i]
        / float(
            teams_average[teams_average["year_ID"] == teams["year_ID"][i]]["salary_x"][
                "mean"
            ]
        )
    )

    agg_sal_plus.append(
        teams["aggregated_salary"][i]
        / float(
            np.mean(teams[teams["year_ID"] == teams["year_ID"][i]]["aggregated_salary"])
        )
    )

player_salary_plus = []
player_salary_plus_agg = []

for i in range(len(players)):
    player_salary_plus.append(
        players["salary_x"][i]
        / float(
            players_average[players_average["year_ID"] == players["year_ID"][i]][
                "salary_x"
            ]["mean"]
        )
    )

    player_salary_plus_agg.append(
        players["aggregated_salary"][i]
        / float(
            players_average[players_average["year_ID"] == players["year_ID"][i]][
                "aggregated_salary"
            ]["mean"]
        )
    )

players["salary_plus"] = player_salary_plus
players["aggregated_salary_plus"] = player_salary_plus_agg

teams["salary_plus"] = pct_teams_average
teams_salary["salary_plus"] = pct_teams_average
teams["aggregated_salary_plus"] = agg_sal_plus
teams["WAR_salary_plus"] = teams["WAR_total"] / teams["aggregated_salary_plus"]

salary_std = []
for i in range(len(teams)):
    team = teams.iloc[i]
    teams_players = (
        players[
            (players["franchID"] == team["franchID"])
            & (players["year_ID"] == team["yearID"])
        ]
        .sort_values("salary_x", ascending=False)
        .head(26)
    )
    std = np.std(teams_players["salary_x"])
    salary_std.append(std)

teams["salary_std"] = salary_std
teams.reset_index(inplace=True)

# commented out code to download final dataframes

# global_variables = [
#     teams,
#     teams_salary,
#     players,
#     players_salary,
#     players_average,
#     teams_average,
# ]
# string_outputs = [
#     "teams",
#     "teams_salary",
#     "players",
#     "players_salary",
#     "players_average",
#     "teams_average",
# ]

# # a here is out-file-path, set to output to project_data
# a = "./project_data/"
# b = r".csv"

# for i in range(len(global_variables)):
#     out = a + string_outputs[i] + b
#     global_variables[i].to_csv(out)
