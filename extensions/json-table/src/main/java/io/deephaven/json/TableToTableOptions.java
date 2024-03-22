//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@BuildableStyle
@Immutable
public abstract class TableToTableOptions {

    public static Builder builder() {
        return ImmutableTableToTableOptions.builder();
    }

    public abstract Table table();

    public abstract Optional<String> columnName();

    public abstract NamedObjectProcessor.Provider processor();

    @Default
    public int chunkSize() {
        return 1024; // todo
    }

    // TODO: empty != null
    // public abstract Map<String, Object> extraAttributes();

    @Default
    public boolean keepOriginalColumns() {
        return false;
    }

    public final Table execute() {
        return executeImpl();
    }

    public interface Builder {
        Builder table(Table table);

        Builder columnName(String columnName);

        Builder processor(NamedObjectProcessor.Provider processor);

        Builder chunkSize(int chunkSize);

        Builder keepOriginalColumns(boolean keepOriginalColumns);

        TableToTableOptions build();

        default Table execute() {
            return build().execute();
        }
    }

    @Check
    final void checkTableIsNotRefreshing() {
        if (table().isRefreshing()) {
            throw new IllegalArgumentException("Only supports non-refreshing tables right now");
        }
    }

    private Table executeImpl() {
        return executeImpl(getColumnDefinition());
    }

    private ColumnDefinition<?> getColumnDefinition() {
        final TableDefinition td = table().getDefinition();
        return columnName().isPresent()
                ? td.getColumn(columnName().get())
                : td.getColumns().iterator().next();
    }

    private <T> Table executeImpl(ColumnDefinition<T> srcDefinition) {
        final ColumnSource<T> srcColumnSource = table().getColumnSource(srcDefinition.getName(), srcDefinition.type());
        final NamedObjectProcessor<? super T> processor = processor().named(srcDefinition.type());
        return executeImpl(srcColumnSource, processor);
    }

    private <T> Table executeImpl(ColumnSource<? extends T> srcColumnSource,
            NamedObjectProcessor<? super T> processor) {
        // todo: option to keep some original table column names in output? would need to flatten.
        // *or*, the new chunks would need to be in the original keyspace.
        final TrackingRowSet srcRowSet = table().getRowSet();
        final TrackingRowSet dstRowSet;
        final List<WritableColumnSource<?>> dstColumnSources;
        final List<Type<?>> dstOutputTypes = processor.processor().outputTypes();
        if (!keepOriginalColumns() || srcRowSet.isFlat()) {
            final long flatSize = srcRowSet.size();
            dstRowSet = RowSetFactory.flat(flatSize).toTracking();
            dstColumnSources = dstOutputTypes
                    .stream()
                    .map(type -> flat(flatSize, type))
                    .collect(Collectors.toList());
        } else {
            dstRowSet = srcRowSet; // todo: do I need to make a copy
            dstColumnSources = dstOutputTypes
                    .stream()
                    .map(TableToTableOptions::sparse)
                    .collect(Collectors.toList());
        }
        final List<WritableColumnSource<?>> dst = dstColumnSources.stream().map(ReinterpretUtils::maybeConvertToWritablePrimitive).collect(Collectors.toList());
        Yep.processAll(srcColumnSource, srcRowSet, processor.processor(), dst, dstRowSet, chunkSize());
        final List<ColumnDefinition<?>> definitions = new ArrayList<>();
        final LinkedHashMap<String, ColumnSource<?>> dstMap = new LinkedHashMap<>();
        if (keepOriginalColumns()) {
            definitions.addAll(table().getDefinition().getColumns());
            dstMap.putAll(table().getColumnSourceMap());
        }
        final List<String> newNames = processor.columnNames();
        for (int i = 0; i < newNames.size(); i++) {
            definitions.add(ColumnDefinition.of(newNames.get(i), dstOutputTypes.get(i)));
            dstMap.put(newNames.get(i), dstColumnSources.get(i));
        }
        // todo: extra attributes empty vs null
        final QueryTable result = new QueryTable(TableDefinition.of(definitions), dstRowSet, dstMap, null, null);
        result.setRefreshing(false);
        if (dstRowSet.isFlat()) {
            result.setFlat();
        }
        return result;
    }

    private static WritableColumnSource<?> flat(long numRows, Type<?> type) {
        // anything special we need to do to support immutable?
        final WritableColumnSource<?> flat =
                InMemoryColumnSource.getImmutableMemoryColumnSource(numRows, type.clazz(), null);
        flat.ensureCapacity(numRows, false);
        return flat;
    }

    private static WritableColumnSource<?> sparse(Type<?> type) {
        return SparseArrayColumnSource.getSparseMemoryColumnSource(type.clazz(), null);
    }
}
