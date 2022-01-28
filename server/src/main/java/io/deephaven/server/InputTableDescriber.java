package io.deephaven.server;

import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.stream.Collectors;

public final class InputTableDescriber {

    public static String describe(InputTableHelper inputTable) {
        if (inputTable.isAppendOnly()) {
            return "Append-only table: " + describe(inputTable.values());
        } else {
            return "Key-backed table: " + describe(inputTable.keys()) + " / " + describe(inputTable.values());
        }
    }

    private static String describe(List<ColumnHeader<?>> headers) {
        return headers.stream().map(InputTableDescriber::describe).collect(Collectors.joining(",", "[", "]"));
    }

    private static String describe(ColumnHeader<?> header) {
        return header.name() + ": " + describe(header.componentType());
    }

    private static String describe(Type<?> type) {
        return type.clazz().getSimpleName(); // todo: do better?
    }
}
