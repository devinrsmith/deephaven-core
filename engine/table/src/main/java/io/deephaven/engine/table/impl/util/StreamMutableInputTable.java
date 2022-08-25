package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.util.config.InputTableStatusListener;
import io.deephaven.engine.util.config.MutableInputTable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class StreamMutableInputTable implements MutableInputTable {

    public static StreamMutableInputTable of(TableDefinition definition, boolean setInputTableAttribute) {
        final TableToStream tableToStream = TableToStream.of(definition);
        final StreamMutableInputTable smit = new StreamMutableInputTable(tableToStream);
        if (setInputTableAttribute) {
            tableToStream.table().setAttribute(Table.INPUT_TABLE_ATTRIBUTE, smit);
        }
        return smit;
    }

    private final TableToStream tableToStream;

    private StreamMutableInputTable(TableToStream tableToStream) {
        this.tableToStream = Objects.requireNonNull(tableToStream);
    }

    @Override
    public void add(Table newData) {
        tableToStream.add(newData);
        // todo: block until data added?
    }

    @Override
    public Table getTable() {
        return tableToStream.table();
    }

    @Override
    public TableDefinition getTableDefinition() {
        return tableToStream.table().getDefinition();
    }

    @Override
    public List<String> getKeyNames() {
        return Collections.emptyList();
    }

    @Override
    public Object[] getEnumsForColumn(String columnName) {
        // todo
        return new Object[0];
    }

    @Override
    public void setRows(Table table, int[] rowArray, Map<String, Object>[] valueArray,
            InputTableStatusListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addRows(Map<String, Object>[] valueArray, boolean allowEdits, InputTableStatusListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getDescription() {
        return StreamMutableInputTable.class.getName();
    }

    @Override
    public boolean canEdit() {
        return false;
    }
}
