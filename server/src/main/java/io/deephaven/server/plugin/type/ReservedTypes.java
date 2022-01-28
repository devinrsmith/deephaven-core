package io.deephaven.server.plugin.type;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.plugin.app.State;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.NewTable;

import java.util.Set;

enum ReservedTypes implements State {
    INSTANCE;

    private static final ColumnHeader<String> HEADER = ColumnHeader.ofString("Name");

    private static final Set<String> RESERVED_TYPE_NAMES_LOWERCASE = Set.of("table", "tablemap", "treetable", "");

    public static boolean isReservedName(String name) {
        return RESERVED_TYPE_NAMES_LOWERCASE.contains(name);
    }

    @Override
    public void insertInto(Consumer consumer) {
        final Table table = InMemoryTable.from(NewTable.of(Column.of(HEADER, RESERVED_TYPE_NAMES_LOWERCASE)));
        consumer.set("names", table, "Reserved names [Name: STRING]");
    }
}
