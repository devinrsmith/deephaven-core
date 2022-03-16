import io.deephaven.march.March

teams = March.get().teams()

matches = March.get().matches()

votes = March.get().votes()

vote_totals = votes.countBy("Count", "RoundOf", "MatchIndex", "Team")

