import io.deephaven.march.March

//teams = March.get().teams()

matches = March.get().matches()

votes = March.get().votes()

current_round = matches.view("RoundOf").lastBy()

vote_totals = votes.countBy("Count", "RoundOf", "MatchIndex", "Team")

match_results = matches
        .naturalJoin(current_round, "", "CurrentRoundOf=RoundOf")
        .naturalJoin(vote_totals, "RoundOf,MatchIndex,TeamA=Team", "TeamACount=Count")
        .naturalJoin(vote_totals, "RoundOf,MatchIndex,TeamB=Team", "TeamBCount=Count")
        .updateView(
                "IsFinal=CurrentRoundOf!=RoundOf",
                "TeamACount=nullToValue(TeamACount, 0)",
                "TeamBCount=nullToValue(TeamBCount, 0)")
        .dropColumns("CurrentRoundOf")

current_winners = match_results
        .view("RoundOf", "MatchIndex", "Team=TeamACount>=TeamBCount?TeamA:TeamB", "IsFinal")