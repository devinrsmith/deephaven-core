package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequence.Iterator;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ChunkSource.FillContext;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.engine.util.TableTools;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.stream.StreamToTableAdapter;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public final class TableToStream {

    public static TableToStream of(TableDefinition definition) {
        final UpdateSourceRegistrar updateSourceRegistrar = UpdateGraphProcessor.DEFAULT; // todo
        final TableToStream smit = new TableToStream();
        final StreamToTableAdapter adapter =
                new StreamToTableAdapter(definition, smit.publisher(), updateSourceRegistrar, "test");
        // adapter.setShutdownCallback(null); // todo
        final Table table = adapter.table();
        smit.setStreamTable(table);
        return smit;
    }

    private StreamConsumer consumer;
    private Table streamTable;

    private TableToStream() {}

    /**
     * Return the {@link Table#STREAM_TABLE_ATTRIBUTE stream} {@link Table table}.
     *
     * @return the stream table
     */
    public Table table() {
        return streamTable;
    }

    public void add(Table newData) {
        addInternal(newData, true);
    }

    public void addSplittable(Table newData) {
        addInternal(newData, false);
    }

    private void addInternal(Table newData, boolean sync) {
        streamTable.getDefinition().checkMutualCompatibility(newData.getDefinition());

        // todo: this is what BaseArrayBackedMutableTable does - do we need select()?
        newData = newData.isRefreshing() ? TableTools.emptyTable(1).snapshot(newData) : newData.select();

        // todo: do we need copy?
        try (final WritableRowSet rowSet = newData.getRowSet().copy()) {
            final int bufferSize = 2048;
            final Iterator it = rowSet.getRowSequenceIterator();
            if (sync) {
                // noinspection SynchronizeOnNonFinalField
                synchronized (consumer) {
                    fillAndHandoff(newData, bufferSize, it);
                }
            } else {
                fillAndHandoff(newData, bufferSize, it);
            }
        }
    }

    private void fillAndHandoff(Table newData, int bufferSize, Iterator it) {
        while (it.hasMore()) {
            final WritableChunk<Values>[] chunks = new WritableChunk[streamTable.numColumns()];
            final RowSequence seq = it.getNextRowSequenceWithLength(bufferSize);
            int i = 0;
            for (String columnName : streamTable.getDefinition().getColumnNames()) {
                final ColumnSource<?> src = newData.getColumnSource(columnName);
                chunks[i] = src.getChunkType().makeWritableChunk(bufferSize);
                try (final FillContext fillContext = src.makeFillContext(bufferSize)) {
                    src.fillChunk(fillContext, chunks[i], seq);
                }
                ++i;
            }
            consumer.accept(chunks);
        }
    }

    private void setStreamTable(Table streamTable) {
        this.streamTable = Objects.requireNonNull(streamTable);
    }

    // Not an interface we need to expose publicly
    private class Publisher implements StreamPublisher {
        @Override
        public void register(@NotNull StreamConsumer consumer) {
            if (TableToStream.this.consumer != null) {
                throw new IllegalStateException("Can not register multiple StreamConsumers.");
            }
            TableToStream.this.consumer = Objects.requireNonNull(consumer);
        }

        @Override
        public void flush() {
            // we don't buffer, nothing to flush
        }
    }

    private Publisher publisher() {
        return new Publisher();
    }
}
