/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.immutable.ImmutableColumnDoubleSource;
import io.deephaven.db.v2.sources.immutable.ImmutableColumnGenericSource;
import io.deephaven.db.v2.sources.immutable.ImmutableColumnIntegerSource;
import io.deephaven.db.v2.sources.immutable.ImmutableDoubleArraySource;
import io.deephaven.db.v2.sources.immutable.ImmutableIntArraySource;
import io.deephaven.db.v2.sources.immutable.ImmutableObjectArraySource;
import io.deephaven.db.v2.utils.Index;

import io.deephaven.qst.Column;
import io.deephaven.qst.ColumnType.Visitor;
import io.deephaven.qst.DoubleType;
import io.deephaven.qst.GenericType;
import io.deephaven.qst.IntType;
import io.deephaven.qst.NewTable;
import io.deephaven.qst.StringType;
import io.deephaven.util.QueryConstants;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class InMemoryTable extends QueryTable {

    private static class ColumnToImmutableDhFormat implements Visitor {

        private final Column<?> column;
        private ColumnSource<?> out;

        public ColumnToImmutableDhFormat(Column<?> column) {
            this.column = Objects.requireNonNull(column);
        }

        public ColumnSource<?> getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(IntType intType) {
            out = new ImmutableColumnIntegerSource(intType.cast(column));
        }

        @Override
        public void visit(StringType stringType) {
            out = new ImmutableColumnGenericSource<>(stringType.cast(column), String.class);

        }

        @Override
        public void visit(DoubleType doubleType) {
            out = new ImmutableColumnDoubleSource(doubleType.cast(column));
        }

        @Override
        public void visit(GenericType<?> genericType) {
            //noinspection unchecked
            out = new ImmutableColumnGenericSource(genericType.cast(column), genericType.clazz()); // todo
        }
    }

    public static InMemoryTable from(NewTable table) {
        final int L = table.numColumns();
        final Map<String, ColumnSource> columns = new LinkedHashMap<>(L);
        for (int i = 0; i < L; ++i) {
            Column<?> column = table.columns().get(i);
            columns.put(column.name(), column.type().walk(new ColumnToImmutableDhFormat(column)).getOut());
        }
        return new InMemoryTable(
            TableDefinition.from(table.header()),
            Index.FACTORY.getFlatIndex(table.size()),
            columns);
    }

    public InMemoryTable(String columnNames[], Object arrayValues[]) {
        super(Index.FACTORY.getFlatIndex(Array.getLength(arrayValues[0])), createColumnsMap(columnNames, arrayValues));
        this.definition.setStorageType(TableDefinition.STORAGETYPE_INMEMORY);
    }

    public InMemoryTable(TableDefinition definition, final int size) {
        super(definition, Index.FACTORY.getFlatIndex( size ),
                createColumnsMap(definition.getColumnNames().toArray(new String[definition.getColumnNames().size()]), Arrays.stream(definition.getColumns()).map(x -> Array.newInstance(x.getDataType(), size)).toArray(Object[]::new)));
        this.definition.setStorageType(TableDefinition.STORAGETYPE_INMEMORY);
    }

    private InMemoryTable(TableDefinition definition, Index index, Map<String, ? extends ColumnSource> columns) {
        super(definition, index, columns);
        this.definition.setStorageType(TableDefinition.STORAGETYPE_INMEMORY);
    }

    private static Map<String, ColumnSource> createColumnsMap(String[] columnNames, Object[] arrayValues) {
        Map<String, ColumnSource> map = new LinkedHashMap<>();
        for (int i = 0; i < columnNames.length; i++) {
            map.put(columnNames[i], ArrayBackedColumnSource.getMemoryColumnSource((arrayValues[i])));
        }
        return map;
    }
}
