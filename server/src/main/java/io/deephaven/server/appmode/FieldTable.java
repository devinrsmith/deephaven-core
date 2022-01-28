package io.deephaven.server.appmode;

import io.deephaven.engine.table.Table;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.column.header.ColumnHeaders2;
import io.deephaven.qst.column.header.ColumnHeaders4;
import io.deephaven.server.InputTableDescriber;
import io.deephaven.server.InputTableHelper;

import java.io.IOException;
import java.util.Objects;

final class FieldTable {

    private static final ColumnHeaders2<String, String> KEY_HEADER = ColumnHeader.ofString("Id")
            .header(ColumnHeader.ofString("Field"));

    private static final ColumnHeaders2<String, String> VALUE_HEADER = ColumnHeader.ofString("Type")
            .header(ColumnHeader.ofString("Description"));

    private static final ColumnHeaders4<String, String, String, String> HEADER = KEY_HEADER.header(VALUE_HEADER);

    public static FieldTable create() {
        return new FieldTable(InputTableHelper.keyBacked(KEY_HEADER, VALUE_HEADER));
    }

    private final InputTableHelper inputTable;

    private FieldTable(InputTableHelper inputTable) {
        this.inputTable = Objects.requireNonNull(inputTable);
    }

    public void add(String id, String field, String type, String description) throws IOException {
        inputTable.add(HEADER.start(1).row(id, field, type, description).newTable());
    }

    public void delete(String id, String field) throws IOException {
        inputTable.delete(KEY_HEADER.start(1).row(id, field).newTable());
    }

    public String describeTable() {
        return InputTableDescriber.describe(inputTable);
    }

    public Table table() {
        return inputTable.table();
    }
}
