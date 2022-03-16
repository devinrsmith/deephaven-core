package io.deephaven.march;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedMutableTable;
import io.deephaven.engine.util.config.MutableInputTable;
import io.deephaven.march.ImmutableRound.Builder;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.column.header.ColumnHeaders4;
import io.deephaven.qst.table.NewTable;

import java.io.IOException;
import java.util.List;

public class Matches {

    private static final ColumnHeaders4<Integer, Integer, Integer, Integer> HEADER = ColumnHeader.ofInt("RoundOf")
            .header(ColumnHeader.ofInt("MatchIndex"))
            .header(ColumnHeader.ofInt("TeamA"))
            .header(ColumnHeader.ofInt("TeamB"));

    private static Round firstRound(Teams teams) {
        final Builder builder = ImmutableRound.builder();
        final List<Team> list = teams.teams();
        final int L = list.size();
        for (int seedA = 0; seedA < L / 2; ++seedA) {
            final int seedB = L - seedA - 1;
            final Team teamA = teams.team(seedA);
            final Team teamB = teams.team(seedB);
            builder.addMatches(Match.of(teamA, teamB));
        }
        return builder.build();
    }

    public static Matches of(Teams teams) throws IOException {
        final Matches matches = new Matches();
        matches.startRound(firstRound(teams));
        return matches;
    }

    private final MutableInputTable handler;
    private final Table readOnlyCopy;

    private Matches() {
        final AppendOnlyArrayBackedMutableTable table = AppendOnlyArrayBackedMutableTable.make(TableDefinition.from(HEADER));
        this.handler = table.mutableInputTable();
        this.readOnlyCopy = table.readOnlyCopy();
    }

    public Table table() {
        return readOnlyCopy;
    }

    public void startRound(Round round) throws IOException {
        final ColumnHeaders4<Integer, Integer, Integer, Integer>.Rows row = HEADER.start(round.size());
        int matchIx = 0;
        for (Match match : round.matches()) {
            row.row(round.numTeams(), matchIx, match.teamA().seed(), match.teamB().seed());
            ++matchIx;
        }
        final NewTable newTable = row.newTable();
        final Table inMemoryTable = InMemoryTable.from(newTable);
        handler.add(inMemoryTable);
    }
}
