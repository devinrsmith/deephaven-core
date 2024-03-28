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
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.BaseTable.ListenerImpl;
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
        return ImmutableTableProcessorOptions.builder();
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
     * The columns from {@link #table()} that should be in the output {@link Table}. When empty, the resulting
     * {@link Table} will be {@link Table#isFlat() flat}. Otherwise, the resulting {@link Table} will be in the keyspace
     * of {@link #table()}.
     */
    public abstract List<String> keepColumns();

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

        Builder addKeepColumns(String element);

        Builder addKeepColumns(String... elements);

        Builder addAllKeepColumns(Iterable<String> elements);

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

    @Check
    final void checkColumnName() {
        if (columnName().isEmpty()) {
            return;
        }
        if (!table().hasColumns(columnName().get())) {
            throw new IllegalArgumentException(String.format("table does not have column '%s'", columnName().get()));
        }
    }

    @Check
    final void checkKeepColumns() {
        if (!table().hasColumns(keepColumns())) {
            throw new IllegalArgumentException(String.format("table does not have all of the keep columns [%s]", String.join(", ", keepColumns())));
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
        final boolean dstFlat = keepColumns().isEmpty() || srcRowSet.isFlat();
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
        TableProcessorImpl.processAll(srcColumnSource, srcRowSet, false, processor.processor(), dst, dstRowSet,
                chunkSize());
        final List<ColumnDefinition<?>> definitions = new ArrayList<>();
        final LinkedHashMap<String, ColumnSource<?>> dstMap = new LinkedHashMap<>();
        final Set<String> usedNames = new HashSet<>();
        for (final String keepColumn : keepColumns()) {
            definitions.add(table().getDefinition().getColumn(keepColumn));
            dstMap.put(keepColumn, table().getColumnSource(keepColumn));
            usedNames.add(keepColumn);
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

//        table().addUpdateListener(new ListenerImpl("todo", table(), result) {
//            @Override
//            public void onUpdate(TableUpdate upstream) {
//
//                upstream.modifiedColumnSet().containsAny();
//
//
//                TableProcessorImpl.processAll(srcColumnSource, upstream.added(), false, processor.processor(), dst, null, chunkSize());
//                //result.notifyListeners(null);
//            }
//        });

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
