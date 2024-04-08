//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.processor;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.ModifiedColumnSet.Transformer;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.BaseTable.CopyAttributeOperation;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
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
            throw new IllegalArgumentException(String.format("table does not have all of the keep columns [%s]",
                    String.join(", ", keepColumns())));
        }
    }

    @Check
    final void checkColumnSpecified() {
        if (columnName().isPresent()) {
            return;
        }
        if (table().numColumns() != 1) {
            throw new IllegalArgumentException(
                    "columnName must be specified when there isn't exactly 1 column in the source table");
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
        final NamedObjectProcessor<? super T> processor;
        try {
            processor = processor().named(srcDefinition.type());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unable to create processor for " + srcDefinition, e);
        }
        return executeImpl(srcDefinition, processor);
    }

    private <T> Table executeImpl(
            ColumnDefinition<? extends T> srcDefinition,
            NamedObjectProcessor<? super T> processor) {
        final TrackingRowSet srcRowSet = table().getRowSet();
        final TrackingRowSet dstRowSet;
        final List<WritableColumnSource<?>> dstColumnSources;
        final boolean dstIsFlat;
        final List<Type<?>> dstOutputTypes = processor.processor().outputTypes();
        if (!table().isRefreshing() && (keepColumns().isEmpty() || srcRowSet.isFlat())) {
            // immutable
            final long flatSize = srcRowSet.size();
            dstIsFlat = true;
            dstRowSet = RowSetFactory.flat(flatSize).toTracking();
            dstColumnSources = dstOutputTypes
                    .stream()
                    .map(type -> immutable(flatSize, type))
                    .collect(Collectors.toList());
        } else if (table().isFlat()) {
            // array
            final long initialSize = srcRowSet.size();
            dstIsFlat = true;
            dstRowSet = srcRowSet; // todo: do we need to make a copy?
            dstColumnSources = dstOutputTypes
                    .stream()
                    .map(type -> array(initialSize, type))
                    .collect(Collectors.toList());
        } else {
            // sparse
            dstIsFlat = false;
            dstRowSet = srcRowSet; // todo: do we need to make a copy?
            dstColumnSources = dstOutputTypes
                    .stream()
                    .map(TableProcessorOptions::sparse)
                    .collect(Collectors.toList());
        }

        final List<ColumnDefinition<?>> definitions = new ArrayList<>();
        final LinkedHashMap<String, ColumnSource<?>> dstMap = new LinkedHashMap<>();
        final Set<String> usedNames = new HashSet<>();
        for (final String keepColumn : keepColumns()) {
            definitions.add(table().getDefinition().getColumn(keepColumn));
            dstMap.put(keepColumn, table().getColumnSource(keepColumn));
            usedNames.add(keepColumn);
        }

        final List<String> newNames = processor.columnNames();
        final LinkedHashMap<String, WritableColumnSource<?>> newDstMap = new LinkedHashMap<>();
        for (int i = 0; i < newNames.size(); i++) {
            final String name = NameValidator.legalizeColumnName(newNames.get(i), usedNames);
            definitions.add(ColumnDefinition.of(name, dstOutputTypes.get(i)));
            dstMap.put(name, dstColumnSources.get(i));
            newDstMap.put(name, ReinterpretUtils.maybeConvertToWritablePrimitive(dstColumnSources.get(i)));
            usedNames.add(name);
        }

        TableProcessorImpl.processAll(columnSource(srcDefinition), srcRowSet, false, processor.processor(),
                newDstMap.values(), dstRowSet, chunkSize());

        // todo: copy attributes according to formula-like pattern?
        // todo: copy blink?
        final QueryTable result = new QueryTable(
                TableDefinition.of(definitions),
                dstRowSet,
                dstMap,
                null,
                null);
        if (dstIsFlat) {
            result.setFlat();
        }
        result.setRefreshing(table().isRefreshing());

        parent().copyAttributes(result, CopyAttributeOperation.View);

        if (table().isRefreshing()) {
            table().addUpdateListener(
                    new ProcessorListener<>("todo", result, srcDefinition, processor.processor(), newDstMap));
        }
        return result;
    }

    private QueryTable parent() {
        return (QueryTable) table();
    }

    private <T> ColumnSource<T> columnSource(ColumnDefinition<T> srcDefinition) {
        return table().getColumnSource(srcDefinition.getName(), srcDefinition.type());
    }

    class ProcessorListener<T> extends BaseTable.ListenerImpl {
        private final ColumnSource<? extends T> srcColumnSource;
        private final ObjectProcessor<? super T> processor;
        private final List<WritableColumnSource<?>> dstColumnSources;
        private final ModifiedColumnSet srcColumnMCS;
        private final ModifiedColumnSet dstColumnsMCS;
        private final Transformer keepColumnsTransformer;
        private final ModifiedColumnSet downstreamMCS;

        ProcessorListener(
                String description,
                QueryTable result,
                ColumnDefinition<? extends T> srcColumnDefinition,
                ObjectProcessor<? super T> processor,
                LinkedHashMap<String, WritableColumnSource<?>> dstColumnSources) {
            super(description, parent(), result);
            if (parent().getRowSet() != result.getRowSet()) {
                throw new IllegalArgumentException();
            }
            this.srcColumnSource = columnSource(srcColumnDefinition);
            this.processor = Objects.requireNonNull(processor);
            this.dstColumnSources = new ArrayList<>(dstColumnSources.values());
            this.srcColumnMCS = parent().newModifiedColumnSet(srcColumnDefinition.getName());
            this.dstColumnsMCS =
                    result.newModifiedColumnSet(dstColumnSources.keySet().toArray(String[]::new));
            keepColumnsTransformer = keepColumns().isEmpty()
                    ? null
                    : parent().newModifiedColumnSetTransformer(result, keepColumns().toArray(String[]::new));
            downstreamMCS = result.getModifiedColumnSetForUpdates();
        }

        @Override
        public void onUpdate(TableUpdate upstream) {
            if (upstream.shifted().nonempty()) {
                throw new IllegalStateException("Not expecting shifts");
            }
            ensureCapacity();
            processRemoved(upstream);
            processAdded(upstream);
            final boolean hasDstColumnMods = processModified(upstream);
            getDependent().notifyListeners(downstreamUpdate(upstream, hasDstColumnMods));
        }

        private void ensureCapacity() {
            final long lastRowKey = getDependent().getRowSet().lastRowKey();
            if (lastRowKey != RowSet.NULL_ROW_KEY) {
                for (final WritableColumnSource<?> dstColumnSource : dstColumnSources) {
                    dstColumnSource.ensureCapacity(lastRowKey + 1, false);
                }
            }
        }

        private void processRemoved(TableUpdate upstream) {
            final RowSet removed = upstream.removed();
            if (removed.isEmpty()) {
                return;
            }
            for (final WritableColumnSource<?> dstColumnSource : dstColumnSources) {
                dstColumnSource.setNull(removed);
            }
        }

        private void processAdded(TableUpdate upstream) {
            final RowSet added = upstream.added();
            if (added.isEmpty()) {
                return;
            }
            TableProcessorImpl.processAll(srcColumnSource, added, false, processor, dstColumnSources, added,
                    chunkSize());
        }

        private boolean processModified(TableUpdate upstream) {
            if (!upstream.modifiedColumnSet().containsAny(srcColumnMCS)) {
                return false;
            }
            if (upstream.modified().isEmpty()) {
                return false;
            }
            // Note: this is a two-pass process, first reading all the columns to see what actually changed, and then
            // only actually acting on the rows that have been modified.
            try (final RowSet modified =
                    ColumnSourceHelper.modified(srcColumnSource, upstream.modified(), chunkSize())) {
                if (modified.isEmpty()) {
                    return false;
                }
                TableProcessorImpl.processAll(srcColumnSource, modified, false, processor, dstColumnSources, modified,
                        chunkSize());
            }
            return true;
        }

        private TableUpdate downstreamUpdate(TableUpdate upstream, boolean hasDstColumnMods) {
            final ModifiedColumnSet mcs = upstream.modifiedColumnSet();
            if (mcs == null || mcs.empty() || (hasDstColumnMods && mcs == ModifiedColumnSet.ALL)) {
                return upstream.acquire();
            }
            updateDownstreamMCS(hasDstColumnMods, mcs);
            return TableUpdateImpl.copy(upstream, downstreamMCS);
        }

        private void updateDownstreamMCS(boolean hasDstColumnMods, ModifiedColumnSet upstreamMCS) {
            downstreamMCS.clear();
            if (keepColumnsTransformer != null) {
                // translate relevant keep columns as dirty
                keepColumnsTransformer.transform(upstreamMCS, downstreamMCS);
            }
            if (hasDstColumnMods) {
                // set all dst columns as dirty
                downstreamMCS.setAll(dstColumnsMCS);
            }
        }
    }

    private static WritableColumnSource<?> array(long numRows, Type<?> type) {
        return ArrayBackedColumnSource.getMemoryColumnSource(numRows, type);
    }

    private static WritableColumnSource<?> immutable(long numRows, Type<?> type) {
        // anything special we need to do to support immutable?
        final WritableColumnSource<?> flat = InMemoryColumnSource.getImmutableMemoryColumnSource(numRows, type);
        flat.ensureCapacity(numRows, false);
        return flat;
    }

    private static WritableColumnSource<?> sparse(Type<?> type) {
        return SparseArrayColumnSource.getSparseMemoryColumnSource(type);
    }
}
