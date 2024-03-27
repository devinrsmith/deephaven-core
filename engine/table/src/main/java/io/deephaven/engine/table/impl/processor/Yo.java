/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.processor;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.BaseTable.ListenerImpl;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import org.jetbrains.annotations.NotNull;

public class Yo<T> extends ListenerImpl implements StreamPublisher {

    private final ObjectProcessor<? super T> processor;
    private final int chunkSize;
    private final ColumnSource<? extends T> source;
    private final boolean oneShot;

    private StreamConsumer consumer;

    @Override
    public void onUpdate(TableUpdate upstream) {
        TableProcessorImpl.processAll(source, upstream.added(), false, processor, chunkSize, consumer, oneShot);
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void flush() {
        // no-op
    }

    @Override
    public void shutdown() {
        // todo
    }
}
