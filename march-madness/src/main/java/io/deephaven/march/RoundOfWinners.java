package io.deephaven.march;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.util.KeyedArrayBackedMutableTable;
import io.deephaven.engine.util.config.MutableInputTable;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.column.header.ColumnHeaders2;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.Collection;

public final class RoundOfWinners {

    public static final ColumnHeaders2<Integer, int[]> HEADER =
            ColumnHeader.ofInt("RoundOf").header(ColumnHeader.of("Team", Type.intType().arrayType()));

    public static RoundOfWinners of() {
        return new RoundOfWinners();
    }

    private final MutableInputTable handler;
    private final Table readOnlyCopy;

    private RoundOfWinners() {
        final KeyedArrayBackedMutableTable table =
                KeyedArrayBackedMutableTable.make(TableDefinition.from(HEADER), "RoundOf");
        handler = table.mutableInputTable();
        readOnlyCopy = table.readOnlyCopy().ungroup("Team");
    }

    public Table table() {
        return readOnlyCopy;
    }

    // must have lock
    public void setRoundOfWinners(int roundOf, Collection<Integer> winners) throws IOException {
        final int expectedWinners = roundOf / 2;
        if (winners.size() != expectedWinners) {
            throw new IllegalStateException("Invalid number of winners");
        }
        handler.add(InMemoryTable.from(HEADER.start(1)
                .row(roundOf, winners.stream().mapToInt(i -> i).toArray())
                .newTable()));
    }
}
