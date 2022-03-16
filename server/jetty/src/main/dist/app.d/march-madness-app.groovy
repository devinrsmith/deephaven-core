import io.deephaven.march.March

teams = March.get().teams()

rounds = March.get().rounds()

votes = March.get().votes()

//votes = MarchMadnessServlet.votes()

//votes_last_by = votes.lastBy("Bracket", "Session", "MatchIndex")

vote_totals = votes.countBy("Count", "Bracket", "MatchIndex", "Team")