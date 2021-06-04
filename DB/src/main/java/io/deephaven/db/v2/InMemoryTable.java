/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.immutable.ImmutableColumnBooleanSource;
import io.deephaven.db.v2.sources.immutable.ImmutableColumnByteSource;
import io.deephaven.db.v2.sources.immutable.ImmutableColumnCharSource;
import io.deephaven.db.v2.sources.immutable.ImmutableColumnDoubleSource;
import io.deephaven.db.v2.sources.immutable.ImmutableColumnFloatSource;
import io.deephaven.db.v2.sources.immutable.ImmutableColumnGenericSource;
import io.deephaven.db.v2.sources.immutable.ImmutableColumnIntegerSource;
import io.deephaven.db.v2.sources.immutable.ImmutableColumnLongSource;
import io.deephaven.db.v2.sources.immutable.ImmutableColumnShortSource;
import io.deephaven.db.v2.utils.Index;

import io.deephaven.qst.column.type.BooleanType;
import io.deephaven.qst.column.type.ByteType;
import io.deephaven.qst.column.type.CharType;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.column.type.ColumnType.Visitor;
import io.deephaven.qst.column.type.DoubleType;
import io.deephaven.qst.column.type.logic.FloatType;
import io.deephaven.qst.column.type.GenericType;
import io.deephaven.qst.column.type.IntType;
import io.deephaven.qst.column.type.LongType;
import io.deephaven.qst.NewTable;
import io.deephaven.qst.column.type.ShortType;
import io.deephaven.qst.column.type.StringType;
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
        public void visit(BooleanType booleanType) {
            out = new ImmutableColumnBooleanSource(Column.cast(booleanType, column));
        }

        @Override
        public void visit(ByteType byteType) {
            out = new ImmutableColumnByteSource(Column.cast(byteType, column));
        }

        @Override
        public void visit(CharType charType) {
            out = new ImmutableColumnCharSource(Column.cast(charType, column));
        }

        @Override
        public void visit(ShortType shortType) {
            out = new ImmutableColumnShortSource(Column.cast(shortType, column));
        }

        @Override
        public void visit(IntType intType) {
            out = new ImmutableColumnIntegerSource(Column.cast(intType, column));
        }

        @Override
        public void visit(LongType longType) {
            out = new ImmutableColumnLongSource(Column.cast(longType, column));
        }

        @Override
        public void visit(StringType stringType) {
            out = new ImmutableColumnGenericSource<>(Column.cast(stringType, column), String.class);
        }

        @Override
        public void visit(FloatType floatType) {
            out = new ImmutableColumnFloatSource(Column.cast(floatType, column));
        }

        @Override
        public void visit(DoubleType doubleType) {
            out = new ImmutableColumnDoubleSource(Column.cast(doubleType, column));
        }

        @Override
        public void visit(GenericType<?> genericType) {
            //noinspection unchecked
            out = new ImmutableColumnGenericSource(Column.cast(genericType, column), genericType.clazz()); // todo
        }
    }

    public static InMemoryTable from(NewTable table) {
        final int L = table.numColumns();
        final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>(L);
        for (int i = 0; i < L; ++i) {
            Column<?> column = table.columns().get(i);
            ColumnSource<?> source = column.type()
                .walk(new ColumnToImmutableDhFormat(column))
                .getOut();
            columns.put(column.name(), source);
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

    private InMemoryTable(TableDefinition definition, Index index, Map<String, ? extends ColumnSource<?>> columns) {
        super(definition, index, columns);
        this.definition.setStorageType(TableDefinition.STORAGETYPE_INMEMORY);
    }

    private static Map<String, ColumnSource<?>> createColumnsMap(String[] columnNames, Object[] arrayValues) {
        Map<String, ColumnSource<?>> map = new LinkedHashMap<>();
        for (int i = 0; i < columnNames.length; i++) {
            map.put(columnNames[i], ArrayBackedColumnSource.getMemoryColumnSource((arrayValues[i])));
        }
        return map;
    }
}
