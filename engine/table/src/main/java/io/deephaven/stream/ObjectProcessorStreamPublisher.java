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

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;

public final class ObjectProcessorStreamPublisher<T> implements StreamPublisher {
    private final ObjectProcessor<T> processor;
    private final Runnable onFlush;
    private final boolean onFlushSynchronized;
    private final Runnable onShutdown;
    private final boolean onShutdownSynchronized;
    private StreamConsumer consumer;

    public ObjectProcessorStreamPublisher(
            ObjectProcessor<T> processor,
            @Nullable Runnable onFlush,
            boolean onFlushSynchronized,
            @Nullable Runnable onShutdown,
            boolean onShutdownSynchronized) {
        this.processor = Objects.requireNonNull(processor);
        this.onFlush = onFlush;
        this.onFlushSynchronized = onFlushSynchronized;
        this.onShutdown = onShutdown;
        this.onShutdownSynchronized = onShutdownSynchronized;
    }

    public ObjectProcessor<T> processor() {
        return processor;
    }

    public synchronized void execute(ColumnSource<T> source, RowSet rowSet, int chunkSize, boolean prev) {
        if (rowSet.isEmpty()) {
            return;
        }
        try (
                final FillContext fillContext = source.makeFillContext(chunkSize);
                final Iterator it = rowSet.getRowSequenceIterator();
                final WritableObjectChunk<T, Values> srcChunk = WritableObjectChunk.makeWritableChunk(chunkSize)) {
            while (it.hasMore()) {
                final RowSequence rowSeq = it.getNextRowSequenceWithLength(chunkSize);
                if (prev) {
                    source.fillPrevChunk(fillContext, srcChunk, rowSeq);
                } else {
                    source.fillChunk(fillContext, srcChunk, rowSeq);
                }
                executeImpl(srcChunk);
            }
        } catch (Throwable t) {
            consumer.acceptFailure(t);
        }
    }

    public synchronized void execute(ObjectChunk<T, ?> source) {
        if (source.size() == 0) {
            return;
        }
        try {
            executeImpl(source);
        } catch (Throwable t) {
            consumer.acceptFailure(t);
        }
    }

    private void executeImpl(ObjectChunk<T, ?> source) {
        final WritableChunk<Values>[] out = makeChunks(source.size());
        processor.processAll(source, Arrays.asList(out));
        consumer.accept(out);
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        this.consumer = Objects.requireNonNull(consumer);
    }

    @Override
    public void flush() {
        if (onFlush == null) {
            return;
        }
        if (onFlushSynchronized) {
            synchronized (this) {
                onFlush.run();
            }
        } else {
            onFlush.run();
        }
    }

    @Override
    public void shutdown() {
        if (onShutdown == null) {
            return;
        }
        if (onShutdownSynchronized) {
            synchronized (this) {
                onShutdown.run();
            }
        } else {
            onShutdown.run();
        }
    }

    private WritableChunk<Values>[] makeChunks(int size) {
        // noinspection unchecked
        return processor.outputTypes()
                .stream()
                .map(ObjectProcessor::chunkType)
                .map(c -> c.makeWritableChunk(size))
                .peek(c -> c.setSize(0))
                .toArray(WritableChunk[]::new);
    }
}
