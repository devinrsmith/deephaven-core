package io.deephaven.server.appmode;

import io.deephaven.engine.table.Table;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.column.header.ColumnHeaders2;
import io.deephaven.server.InputTableDescriber;
import io.deephaven.server.InputTableHelper;

import java.io.IOException;
import java.util.Objects;

final class ApplicationTable {

    private static final ColumnHeaders2<String, String> HEADER = ColumnHeader.ofString("Id")
            .header(ColumnHeader.ofString("Name"));

    public static ApplicationTable create() {
        return new ApplicationTable(InputTableHelper.appendOnly(HEADER));
    }

    private final InputTableHelper inputTable;

    private ApplicationTable(InputTableHelper inputTable) {
        this.inputTable = Objects.requireNonNull(inputTable);
    }

    public void add(String id, String name) throws IOException {
        inputTable.add(HEADER.start(1).row(id, name).newTable());
    }

    public String describeTable() {
        return InputTableDescriber.describe(inputTable);
    }

    public Table table() {
        return inputTable.table();
    }
}
