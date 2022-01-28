package io.deephaven.server.plugin.type;

import io.deephaven.plugin.app.State;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeRegistration;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.server.InputTableDescriber;
import io.deephaven.server.InputTableHelper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

final class ObjectTypesTable implements State, ObjectTypeRegistration.Listener {
    private static final ColumnHeader<String> OBJECT_TYPE_HEADER = ColumnHeader.ofString("Name");

    public static ObjectTypesTable create() {
        return new ObjectTypesTable(InputTableHelper.appendOnly(OBJECT_TYPE_HEADER));
    }

    private final InputTableHelper inputTable;

    private ObjectTypesTable(InputTableHelper inputTable) {
        this.inputTable = Objects.requireNonNull(inputTable);
    }

    @Override
    public void onRegistered(ObjectType objectType) {
        try {
            inputTable.add(OBJECT_TYPE_HEADER.start(1).row(objectType.name()).newTable());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void insertInto(Consumer consumer) {
        consumer.set("objectTypes", inputTable.table(), InputTableDescriber.describe(inputTable));
    }
}
