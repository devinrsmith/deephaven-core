package io.deephaven.server.appmode;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.ApplicationState.Listener;
import io.deephaven.appmode.Field;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.util.KeyedArrayBackedMutableTable;
import io.deephaven.engine.util.config.MutableInputTable;
import io.deephaven.plugin.app.State;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.column.header.ColumnHeaders2;
import io.deephaven.qst.column.header.ColumnHeaders4;
import io.deephaven.server.object.TypeLookup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.Optional;

final class FieldState implements Listener, State {

    private static final ColumnHeaders2<String, String> KEY_HEADER = ColumnHeader.ofString("Id")
            .header(ColumnHeader.ofString("Field"));

    private static final ColumnHeaders4<String, String, String, String> HEADER = KEY_HEADER
            .header(ColumnHeader.ofString("Type"))
            .header(ColumnHeader.ofString("Description"));

    public static FieldState create(TypeLookup typeLookup) {
        final KeyedArrayBackedMutableTable table = KeyedArrayBackedMutableTable.make(TableDefinition.from(HEADER),
                KEY_HEADER.tableHeader().columnNames().toArray(String[]::new));
        final MutableInputTable inputTable = (MutableInputTable) table.getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
        final Table readOnly = table.copy(false);
        table.copyAttributes(readOnly, x -> !Table.INPUT_TABLE_ATTRIBUTE.equals(x));
        return new FieldState(inputTable, readOnly, typeLookup);
    }

    private final MutableInputTable inputTable;
    private final Table readOnlyTable;
    private final TypeLookup typeLookup;

    private FieldState(MutableInputTable inputTable, Table readOnlyTable, TypeLookup typeLookup) {
        this.inputTable = Objects.requireNonNull(inputTable);
        this.readOnlyTable = Objects.requireNonNull(readOnlyTable);
        this.typeLookup = Objects.requireNonNull(typeLookup);
    }

    @Override
    public void onNewField(ApplicationState app, Field<?> field) {
        final Optional<String> type = typeLookup.type(field.value());
        // todo: app name should be elsewhere
        final InMemoryTable entry = InMemoryTable.from(HEADER.start(1)
                .row(app.id(), field.name(), type.orElse(null), field.description().orElse(null))
                .newTable());
        try {
            inputTable.add(entry);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void onRemoveField(ApplicationState app, Field<?> field) {
        final InMemoryTable key = InMemoryTable.from(KEY_HEADER.start(1)
                .row(app.id(), field.name())
                .newTable());
        try {
            inputTable.delete(key);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void insertInto(Consumer consumer) {
        consumer.set("fields", table(),
                "Application Fields [Id: STRING, Field: STRING, Type: STRING, Description: STRING]");
    }

    public Table table() {
        return readOnlyTable;
    }
}
