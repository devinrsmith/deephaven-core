import io.deephaven.march.March

import static io.deephaven.api.agg.spec.AggSpec.sortedLast
import static io.deephaven.engine.util.TableTools.merge

teams = March.get().teamsTable()

rounds = March.get().roundsTable()

matches = March.get().matchesTable()

match_indices = merge(
        matches.view("RoundOf", "Team=TeamA", "MatchIndex"),
        matches.view("RoundOf", "Team=TeamB", "MatchIndex"))

votes_pre = March.get().votesTable()

votes = votes_pre
        .naturalJoin(match_indices, "RoundOf,Team", "MatchIndex")
        .aggAllBy(sortedLast("Timestamp"), "Session", "RoundOf", "MatchIndex")

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

winners = match_results
        .view("RoundOf", "MatchIndex", "Team=TeamACount>=TeamBCount?TeamA:TeamB", "IsFinal")

locked_winners = winners.where("IsFinal").view("RoundOf", "MatchIndex", "Team")

potential_winners = winners.where("!IsFinal").view("RoundOf", "MatchIndex", "Team")

March.start(potential_winners)

//start_next_round = {
//    March.get().matches().nextRound(potential_winners)
//}
