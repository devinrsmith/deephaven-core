import io.deephaven.march.March

import static io.deephaven.api.agg.spec.AggSpec.sortedLast
import static io.deephaven.engine.util.TableTools.merge

teams = March.get().teamsTable()

rounds = March.get().roundsTable()

matches = March.get().matchesTable()

ip_blocklist = March.get().ipBlocklist()

round_winners = March.get().roundWinnersTable()

current_round = March.get().currentRoundTable()

finished_rounds = round_winners.selectDistinct("RoundOf")

last_round = finished_rounds.minBy()

match_indices = merge(
        matches.view("RoundOf", "Team=TeamA", "MatchIndex"),
        matches.view("RoundOf", "Team=TeamB", "MatchIndex"))

votes_pre = March.get().votesTable()

votes = votes_pre
        .whereNotIn(ip_blocklist, "Ip")
        .naturalJoin(match_indices, "RoundOf,Team", "MatchIndex")
        .aggAllBy(sortedLast("Timestamp"), "Session", "RoundOf", "MatchIndex")

vote_totals = votes.countBy("Count", "RoundOf", "MatchIndex", "Team")

match_results = matches
        .naturalJoin(finished_rounds.updateView("IsFinal=true"), "RoundOf", "IsFinal")
        .naturalJoin(vote_totals, "RoundOf,MatchIndex,TeamA=Team", "TeamACount=Count")
        .naturalJoin(vote_totals, "RoundOf,MatchIndex,TeamB=Team", "TeamBCount=Count")
        .updateView(
                "IsFinal=nullToValue(IsFinal, false)",
                "TeamACount=nullToValue(TeamACount, 0)",
                "TeamBCount=nullToValue(TeamBCount, 0)")

winners = match_results
        .view("RoundOf", "MatchIndex", "Team=TeamACount>=TeamBCount?TeamA:TeamB", "IsFinal")

potential_winners = winners.where("!IsFinal").view("RoundOf", "MatchIndex", "Team")

March.start(potential_winners)

//start_next_round = {
//    March.get().matches().nextRound(32, potential_winners)
//}
