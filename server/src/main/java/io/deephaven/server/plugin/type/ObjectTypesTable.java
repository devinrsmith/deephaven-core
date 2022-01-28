package io.deephaven.server.plugin.type;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedMutableTable;
import io.deephaven.engine.util.config.MutableInputTable;
import io.deephaven.plugin.app.State;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.NewTable;

import java.io.IOException;
import java.util.Objects;

final class ObjectTypesTable implements State {
    private static final ColumnHeader<String> OBJECT_TYPE_HEADER = ColumnHeader.ofString("Name");

    private static AppendOnlyArrayBackedMutableTable make() {
        return AppendOnlyArrayBackedMutableTable.make(TableDefinition.from(OBJECT_TYPE_HEADER));
    }

    public static ObjectTypesTable create() {
        final AppendOnlyArrayBackedMutableTable table = make();
        return new ObjectTypesTable(table.mutableInputTable(), table.readOnlyCopy());
    }

    private final MutableInputTable inputTable;
    private final Table readOnlyTable;

    private ObjectTypesTable(MutableInputTable inputTable, Table readOnlyTable) {
        this.inputTable = Objects.requireNonNull(inputTable);
        this.readOnlyTable = Objects.requireNonNull(readOnlyTable);
    }

    public void add(ObjectType objectType) throws IOException {
        final NewTable entry = OBJECT_TYPE_HEADER.start(1).row(objectType.name()).newTable();
        inputTable.add(InMemoryTable.from(entry));
    }

    public Table table() {
        return readOnlyTable;
    }

    @Override
    public void insertInto(Consumer consumer) {
        consumer.set("names", readOnlyTable, "ObjectType names [Name: STRING]");
    }
}
