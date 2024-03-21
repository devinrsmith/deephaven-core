/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequence.Iterator;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink.FillFromContext;
import io.deephaven.engine.table.ChunkSource.FillContext;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import io.deephaven.util.SafeCloseable;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@BuildableStyle
@Immutable
public abstract class TableToTableOptions<T> {

    public static <T> Builder<T> builder() {
        return ImmutableTableToTableOptions.builder();
    }

    public abstract Table table();

    public abstract Optional<String> columnName();

    public abstract Type<? extends T> columnType();

    // todo: change this to provider, get rid of type from interface?
    public abstract NamedObjectProcessor<? super T> processor();

    @Default
    public int chunkSize() {
        return 1024; // todo
    }

    // TODO: empty != null
    // public abstract Map<String, Object> extraAttributes();

    public final Table execute() {
        return executeImpl();
    }

    public interface Builder<T> {
        Builder<T> table(Table table);

        Builder<T> columnName(String columnName);

        Builder<T> columnType(Type<? extends T> columnType);

        Builder<T> processor(NamedObjectProcessor<? super T> processor);

        Builder<T> chunkSize(int chunkSize);

        TableToTableOptions<T> build();

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

    private ColumnSource<? extends T> columnSource() {
        // todo, type
        final String columnName = columnName().orElse(table().getDefinition().getColumnNames().get(0));
        return table().getColumnSource(columnName, columnType().clazz());
    }

    private Table executeImpl() {
        // todo: option to keep some original table column names in output? would need to flatten.
        // *or*, the new chunks would need to be in the original keyspace.
        final long numRows = table().size();
        final ObjectProcessor<? super T> objectProcessor = processor().processor();
        final ColumnSource<? extends T> source = columnSource();
        final int numColumns = objectProcessor.size();
        final List<WritableColumnSource<?>> newColumns = objectProcessor.outputTypes()
                .stream()
                .map(type -> ArrayBackedColumnSource.getMemoryColumnSource(numRows, type.clazz()))
                .collect(Collectors.toList());
        long pos = 0;
        {
            final List<FillFromContext> fillFromContexts = newColumns.stream()
                    .map(cs -> cs.makeFillFromContext(chunkSize()))
                    .collect(Collectors.toList());
            final List<WritableChunk<Values>> intermediateChunks = objectProcessor.outputTypes()
                    .stream()
                    .map(ObjectProcessor::chunkType)
                    .map(o -> o.<Values>makeWritableChunk(chunkSize()))
                    .collect(Collectors.toList());
            try (
                    final WritableObjectChunk<? extends T, Values> src = WritableObjectChunk.makeWritableChunk(1024);
                    final FillContext context = source.makeFillContext(chunkSize());
                    final Iterator it = table().getRowSet().getRowSequenceIterator()) {
                while (it.hasMore()) {
                    final RowSequence rowSeq = it.getNextRowSequenceWithLength(chunkSize());
                    final int rowSeqSize = rowSeq.intSize();
                    final long lastRowKey = pos + rowSeqSize - 1;
                    source.fillChunk(context, src, rowSeq);
                    for (WritableChunk<?> chunk : intermediateChunks) {
                        chunk.setSize(0);
                    }
                    //noinspection rawtypes,unchecked
                    objectProcessor.processAll(src, (List) intermediateChunks);
                    for (int i = 0; i < numColumns; ++i) {
                        if (intermediateChunks.get(i).size() != rowSeqSize) {
                            throw new IllegalStateException();
                        }
                        // todo: share this for all fillFromChunk?
                        try (final RowSequence nextRows = RowSequenceFactory.forRange(pos, lastRowKey)) {
                            newColumns.get(i).fillFromChunk(fillFromContexts.get(i), intermediateChunks.get(i), nextRows);
                        }
                    }
                    pos = lastRowKey + 1;
                }
            } finally {
                SafeCloseable.closeAll(Stream.concat(intermediateChunks.stream(), fillFromContexts.stream()));
            }
        }
        if (pos != numRows) {
            throw new IllegalStateException();
        }
        final List<String> columnNames = processor().columnNames();
        final TableDefinition tableDefinition = TableDefinition.from(columnNames, objectProcessor.outputTypes());
        final LinkedHashMap<String, ColumnSource<?>> newColumnsMap = new LinkedHashMap<>(numColumns);
        for (int i = 0; i < numColumns; i++) {
            newColumnsMap.put(columnNames.get(i), newColumns.get(i));
        }
        // todo: extra attributes empty vs null
        final QueryTable result = new QueryTable(
                tableDefinition,
                RowSetFactory.flat(numRows).toTracking(),
                newColumnsMap,
                null,
                null);
        result.setRefreshing(false);
        result.setFlat();
        return result;
    }
}
