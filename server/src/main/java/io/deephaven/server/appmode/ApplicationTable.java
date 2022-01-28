package io.deephaven.server.appmode;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.plugin.app.State;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.column.header.ColumnHeaders2;
import io.deephaven.server.InputTableDescriber;
import io.deephaven.server.InputTableHelper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

final class ApplicationTable implements State, Applications.Listener {

    private static final ColumnHeaders2<String, String> HEADER = ColumnHeader.ofString("Id")
            .header(ColumnHeader.ofString("Name"));

    public static ApplicationTable create() {
        return new ApplicationTable(InputTableHelper.appendOnly(HEADER));
    }

    private final InputTableHelper inputTable;

    private ApplicationTable(InputTableHelper inputTable) {
        this.inputTable = Objects.requireNonNull(inputTable);
    }

    @Override
    public void onApplicationLoad(ApplicationState app) {
        try {
            add(app.id(), app.name());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void add(String id, String name) throws IOException {
        inputTable.add(HEADER.start(1).row(id, name).newTable());
    }

    @Override
    public void insertInto(Consumer consumer) {
        consumer.set("application", inputTable.table(), InputTableDescriber.describe(inputTable));
    }
}
