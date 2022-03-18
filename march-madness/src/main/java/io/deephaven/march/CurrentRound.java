package io.deephaven.march;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.util.KeyedArrayBackedMutableTable;
import io.deephaven.engine.util.config.MutableInputTable;
import io.deephaven.qst.column.header.ColumnHeader;

import java.io.IOException;

public final class CurrentRound {

    private static final ColumnHeader<Integer> HEADER = ColumnHeader.ofInt("RoundOf");

    public static CurrentRound of() {
        return new CurrentRound();
    }

    private final MutableInputTable handler;
    private final Table readOnlyCopy;

    private CurrentRound() {
        // no key columns
        final KeyedArrayBackedMutableTable table = KeyedArrayBackedMutableTable.make(TableDefinition.from(HEADER));
        handler = table.mutableInputTable();
        readOnlyCopy = table.readOnlyCopy();
    }

    public Table table() {
        return readOnlyCopy;
    }

    // must have lock
    public void setRoundOf(int roundOf) throws IOException {
        handler.add(InMemoryTable.from(HEADER.start(1).row(roundOf).newTable()));
    }
}
