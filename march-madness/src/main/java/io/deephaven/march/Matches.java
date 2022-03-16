package io.deephaven.march;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedMutableTable;
import io.deephaven.engine.util.config.MutableInputTable;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.column.header.ColumnHeaders4;
import io.deephaven.qst.table.NewTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Matches {

    private static final ColumnHeaders4<Integer, Integer, Integer, Integer> HEADER = ColumnHeader.ofInt("RoundOf")
            .header(ColumnHeader.ofInt("MatchIndex"))
            .header(ColumnHeader.ofInt("TeamA"))
            .header(ColumnHeader.ofInt("TeamB"));

    public static Matches of(Teams teams) throws IOException {
        final Matches matches = new Matches();
        final Round firstRound = teams.toSeededRound();
        final Lock lock = matches.writeLock();
        lock.lock();
        try {
            matches.startRound(firstRound);
        } finally {
            lock.unlock();
        }
        return matches;
    }

    private final MutableInputTable handler;
    private final Table readOnlyCopy;
    private final List<Round> rounds;

    private final ReadWriteLock lock;

    private Matches() {
        final AppendOnlyArrayBackedMutableTable table =
                AppendOnlyArrayBackedMutableTable.make(TableDefinition.from(HEADER));
        this.handler = table.mutableInputTable();
        this.readOnlyCopy = table.readOnlyCopy();
        this.lock = new ReentrantReadWriteLock(false);
        this.rounds = new ArrayList<>();
    }

    public Table table() {
        return readOnlyCopy;
    }

    public Lock writeLock() {
        return lock.writeLock();
    }

    public Lock readLock() {
        return lock.readLock();
    }

    // caller must have write lock
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
        rounds.add(round);
    }

    // caller must have read lock
    public OptionalInt isValid(int roundOf, int teamId) {
        final Round latestRound = rounds.get(rounds.size() - 1);
        if (latestRound.numTeams() == roundOf && latestRound.hasTeam(teamId)) {
            return OptionalInt.of(latestRound.matchIndex(teamId));
        }
        return OptionalInt.empty();
    }
}
