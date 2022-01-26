package io.deephaven.server.plugin.type;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedMutableTable;
import io.deephaven.engine.util.config.MutableInputTable;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.NewTable;

import java.io.IOException;
import java.util.Objects;

final class ObjectTypesTable {
    private static final ColumnHeader<String> OBJECT_TYPE_HEADER = ColumnHeader.ofString("Name");

    public static ObjectTypesTable create() {
        final AppendOnlyArrayBackedMutableTable table = createTable();
        return new ObjectTypesTable(table, copyDropInputTable(table));
    }

    private static AppendOnlyArrayBackedMutableTable createTable() {
        return AppendOnlyArrayBackedMutableTable.make(TableDefinition.from(OBJECT_TYPE_HEADER));
    }

    private static Table copyDropInputTable(QueryTable input) {
        final Table copy = input.copy(false);
        input.copyAttributes(copy, x -> !Table.INPUT_TABLE_ATTRIBUTE.equals(x));
        return copy;
    }

    private final AppendOnlyArrayBackedMutableTable table;
    private final Table readOnlyTable;

    private ObjectTypesTable(AppendOnlyArrayBackedMutableTable table, Table readOnlyTable) {
        this.table = Objects.requireNonNull(table);
        this.readOnlyTable = Objects.requireNonNull(readOnlyTable);
    }

    public void add(ObjectType objectType) throws IOException {
        final MutableInputTable inputTable =
                Objects.requireNonNull((MutableInputTable) table.getAttribute(Table.INPUT_TABLE_ATTRIBUTE));
        final NewTable entry = OBJECT_TYPE_HEADER.start(1).row(objectType.name()).newTable();
        inputTable.add(InMemoryTable.from(entry));
    }

    public Table table() {
        return readOnlyTable;
    }
}
