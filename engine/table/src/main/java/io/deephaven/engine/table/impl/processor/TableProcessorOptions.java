//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.processor;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@BuildableStyle
@Immutable
public abstract class TableProcessorOptions {

    public static Builder builder() {
        return ImmutableTableToTableOptions.builder();
    }

    /**
     * The input {@link Table}. Must be a static table.
     */
    public abstract Table table();

    /**
     * The input column name. When unset, the first column from {@link #table()} will be used.
     */
    public abstract Optional<String> columnName();

    /**
     * The named object processor provider. Must be capable of handling the input column type.
     */
    public abstract NamedObjectProcessor.Provider processor();

    /**
     * If the columns from {@link #table()} should be in the output {@link Table}. By default, is {@code false}. When
     * {@code false}, the resulting {@link Table} will be {@link Table#isFlat() flat}. Otherwise, the resulting
     * {@link Table} will be in the keyspace of {@link #table()}. As such, callers may want to {@link Table#flatten()
     * flatten} the input {@link #table()} when this is {@code true}.
     */
    @Default
    public boolean keepOriginalColumns() {
        return false;
    }

    /**
     * The chunk size used to iterate through {@link #table()}. By default, is
     * {@value ArrayBackedColumnSource#BLOCK_SIZE}.
     */
    @Default
    public int chunkSize() {
        return ArrayBackedColumnSource.BLOCK_SIZE;
    }

    /**
     * Creates a new {@link Table} based on the arguments from {@code this}.
     */
    public final Table execute() {
        return executeImpl();
    }

    public interface Builder {
        Builder table(Table table);

        Builder columnName(String columnName);

        Builder processor(NamedObjectProcessor.Provider processor);

        Builder keepOriginalColumns(boolean keepOriginalColumns);

        Builder chunkSize(int chunkSize);

        TableProcessorOptions build();

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

    private <T> Table executeImpl(
            ColumnSource<? extends T> srcColumnSource,
            NamedObjectProcessor<? super T> processor) {
        final TrackingRowSet srcRowSet = table().getRowSet();
        final TrackingRowSet dstRowSet;
        final List<WritableColumnSource<?>> dstColumnSources;
        final boolean dstFlat = !keepOriginalColumns() || srcRowSet.isFlat();
        final List<Type<?>> dstOutputTypes = processor.processor().outputTypes();
        if (dstFlat) {
            // todo: we could also do this if we know the source is dense or contiguous (potentially w/ shift)
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
                    .map(TableProcessorOptions::sparse)
                    .collect(Collectors.toList());
        }
        final List<WritableColumnSource<?>> dst = dstColumnSources.stream()
                .map(ReinterpretUtils::maybeConvertToWritablePrimitive)
                .collect(Collectors.toList());
        TableProcessorImpl.processAll(srcColumnSource, srcRowSet, processor.processor(), dst, dstRowSet, chunkSize());
        final List<ColumnDefinition<?>> definitions = new ArrayList<>();
        final LinkedHashMap<String, ColumnSource<?>> dstMap = new LinkedHashMap<>();
        final Set<String> usedNames = new HashSet<>();
        if (keepOriginalColumns()) {
            definitions.addAll(table().getDefinition().getColumns());
            dstMap.putAll(table().getColumnSourceMap());
            usedNames.addAll(table().getDefinition().getColumnNames());
        }
        final List<String> newNames = processor.columnNames();
        for (int i = 0; i < newNames.size(); i++) {
            final String name = NameValidator.legalizeColumnName(newNames.get(i), usedNames);
            definitions.add(ColumnDefinition.of(name, dstOutputTypes.get(i)));
            dstMap.put(name, dstColumnSources.get(i));
            usedNames.add(name);
        }
        final QueryTable result = new QueryTable(
                TableDefinition.of(definitions),
                dstRowSet,
                dstMap,
                null,
                null);
        result.setRefreshing(false);
        if (dstFlat) {
            result.setFlat();
        }
        return result;
    }

    private static WritableColumnSource<?> flat(long numRows, Type<?> type) {
        // anything special we need to do to support immutable?
        final WritableColumnSource<?> flat = InMemoryColumnSource.getImmutableMemoryColumnSource(numRows, type);
        flat.ensureCapacity(numRows, false);
        return flat;
    }

    private static WritableColumnSource<?> sparse(Type<?> type) {
        return SparseArrayColumnSource.getSparseMemoryColumnSource(type);
    }
}
