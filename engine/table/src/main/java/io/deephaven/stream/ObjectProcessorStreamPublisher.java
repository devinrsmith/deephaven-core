//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.stream;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequence.Iterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSource.FillContext;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.processor.ObjectProcessor;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class ObjectProcessorStreamPublisher<T> implements StreamPublisher {
    private final ObjectProcessor<T> processor;
    private StreamConsumer consumer;
    private volatile boolean shutdown;

    public ObjectProcessorStreamPublisher(ObjectProcessor<T> processor) {
        this.processor = Objects.requireNonNull(processor);
    }

    public ObjectProcessor<T> processor() {
        return processor;
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public synchronized void execute(ColumnSource<T> source, RowSet rowSet, int chunkSize, boolean prev) {
        if (rowSet.isEmpty()) {
            return;
        }
        try (
                final WritableObjectChunk<T, Values> dst = WritableObjectChunk.makeWritableChunk(chunkSize);
                final FillContext context = source.makeFillContext(chunkSize);
                final Iterator it = rowSet.getRowSequenceIterator()) {
            while (it.hasMore()) {
                final RowSequence rowSeq = it.getNextRowSequenceWithLength(chunkSize);
                if (prev) {
                    source.fillPrevChunk(context, dst, rowSeq);
                } else {
                    source.fillChunk(context, dst, rowSeq);
                }
                execute(dst);
            }
        }
    }

    public synchronized void execute(ObjectChunk<T, ?> source) {
        final List<WritableChunk<?>> out = makeChunks(source.size());
        processor.processAll(source, out);
        // noinspection unchecked
        consumer.accept(out.toArray(WritableChunk[]::new));
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        this.consumer = Objects.requireNonNull(consumer);
    }

    @Override
    public synchronized void flush() {
        // synchronized to wait for any executes
    }

    @Override
    public void shutdown() {
        shutdown = true;
    }

    private List<WritableChunk<?>> makeChunks(int size) {
        return processor.outputTypes()
                .stream()
                .map(ObjectProcessor::chunkType)
                .map(c -> c.makeWritableChunk(size))
                .peek(c -> c.setSize(0))
                .collect(Collectors.toList());
    }
}
