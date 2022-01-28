package io.deephaven.server;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedMutableTable;
import io.deephaven.engine.table.impl.util.KeyedArrayBackedMutableTable;
import io.deephaven.engine.util.config.MutableInputTable;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

// TODO: move
public final class InputTableHelper {

    public static InputTableHelper appendOnly(Iterable<ColumnHeader<?>> values) {
        final List<ColumnHeader<?>> valueList =
                StreamSupport.stream(values.spliterator(), false).collect(Collectors.toUnmodifiableList());
        final AppendOnlyArrayBackedMutableTable appendOnly =
                AppendOnlyArrayBackedMutableTable.make(TableDefinition.from(valueList));
        return new InputTableHelper(null, valueList, appendOnly.mutableInputTable(), appendOnly.readOnlyCopy());
    }

    public static InputTableHelper keyBacked(Iterable<ColumnHeader<?>> keys, Iterable<ColumnHeader<?>> values) {
        final List<ColumnHeader<?>> keyList =
                StreamSupport.stream(keys.spliterator(), false).collect(Collectors.toUnmodifiableList());
        final List<ColumnHeader<?>> valueList =
                StreamSupport.stream(values.spliterator(), false).collect(Collectors.toUnmodifiableList());
        final TableHeader fullHeader = TableHeader.builder().addAllHeaders(keyList).addAllHeaders(valueList).build();
        final KeyedArrayBackedMutableTable keyBacked = KeyedArrayBackedMutableTable.make(
                TableDefinition.from(fullHeader), keyList.stream().map(ColumnHeader::name).toArray(String[]::new));
        return new InputTableHelper(keyList, valueList, keyBacked.mutableInputTable(), keyBacked.readOnlyCopy());
    }

    private final List<ColumnHeader<?>> keys;
    private final List<ColumnHeader<?>> values;
    private final MutableInputTable inputTable;
    private final Table readOnlyTable;

    private InputTableHelper(List<ColumnHeader<?>> keys, List<ColumnHeader<?>> values, MutableInputTable inputTable,
                             Table readOnlyTable) {
        this.keys = keys;
        this.values = Objects.requireNonNull(values);
        this.inputTable = Objects.requireNonNull(inputTable);
        this.readOnlyTable = Objects.requireNonNull(readOnlyTable);
    }

    public boolean isAppendOnly() {
        return keys == null;
    }

    public List<ColumnHeader<?>> keys() {
        return keys;
    }

    public List<ColumnHeader<?>> values() {
        return values;
    }

    public Table table() {
        return readOnlyTable;
    }

    public void add(NewTable entries) throws IOException {
        inputTable.add(InMemoryTable.from(entries));
    }

    public void delete(NewTable deleteKeys) throws IOException {
        inputTable.delete(InMemoryTable.from(deleteKeys));
    }
}
