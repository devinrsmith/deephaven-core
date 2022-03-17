package io.deephaven.march;

import io.deephaven.csv.CsvTools;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedMutableTable;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.config.MutableInputTable;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.column.header.ColumnHeaders4;
import io.deephaven.qst.table.NewTable;
import io.deephaven.util.locks.AwareFunctionalLock;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

public final class Matches {

    private static final ColumnHeaders4<Integer, Integer, Integer, Integer> HEADER = ColumnHeader.ofInt("RoundOf")
            .header(ColumnHeader.ofInt("MatchIndex"))
            .header(ColumnHeader.ofInt("TeamA"))
            .header(ColumnHeader.ofInt("TeamB"));

    public static Matches of(UpdateGraphProcessor ugp, Path winnersDir) throws IOException {
        return new Matches(ugp, winnersDir);
    }

    private final UpdateGraphProcessor ugp;
    private final MutableInputTable handler;
    private final Table readOnlyCopy;
    private final List<Round> rounds;
    private final Path winnersDir;

    // private final ReadWriteLock lock;

    private Matches(UpdateGraphProcessor ugp, Path winnersDir) {
        this.ugp = Objects.requireNonNull(ugp);
        this.winnersDir = Objects.requireNonNull(winnersDir);

        final AppendOnlyArrayBackedMutableTable table =
                AppendOnlyArrayBackedMutableTable.make(TableDefinition.from(HEADER));
        this.handler = table.mutableInputTable();
        this.readOnlyCopy = table.readOnlyCopy();
        // this.lock = new ReentrantReadWriteLock(false);
        this.rounds = new ArrayList<>();
    }

    public Table table() {
        return readOnlyCopy;
    }

    // caller must have read lock
    OptionalInt isValid(int roundOf, int teamId) {
        final Round latestRound = rounds.get(rounds.size() - 1);
        if (latestRound.numTeams() == roundOf && latestRound.hasTeam(teamId)) {
            return OptionalInt.of(latestRound.matchIndex(teamId));
        }
        return OptionalInt.empty();
    }

    public void init(Round initialRound) throws IOException, CsvReaderException {
        final AwareFunctionalLock lock = ugp.exclusiveLock();
        lock.lock();
        try {
            startRound(initialRound);
            Round currentRound = initialRound;
            while (currentRound.numTeams() > 2) {
                final Optional<Set<Integer>> winners = winnersForRound(currentRound);
                if (winners.isEmpty()) {
                    break;
                }
                final Round nextRound = currentRound.nextRound(winners.get());
                startRound(nextRound);
                currentRound = nextRound;
            }
        } finally {
            lock.unlock();
        }
    }

    private Optional<Set<Integer>> winnersForRound(Round currentRound) throws CsvReaderException {
        final Path winners = csvPathForWinnersOf(currentRound);
        if (!Files.exists(winners)) {
            return Optional.empty();
        }
        final Table table = CsvTools.readCsv(winners);
        final Set<Integer> winningIds = new HashSet<>();
        final int L = currentRound.size();
        if (table.size() != L) {
            throw new IllegalStateException();
        }
        final ColumnSource<Integer> source = table.getColumnSource("Team");
        for (int i = 0; i < L; ++i) {
            winningIds.add(source.getInt(i));
        }
        return Optional.of(winningIds);
    }

    // caller must have write lock
    private void startRound(Round round) throws IOException {
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

    private Path csvPathForWinnersOf(Round latestRound) {
        return winnersDir.resolve(String.format("%d.csv", latestRound.numTeams()));
    }
}
