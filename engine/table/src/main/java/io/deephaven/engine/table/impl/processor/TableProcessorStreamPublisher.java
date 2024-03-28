//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.processor;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.BaseTable.ListenerImpl;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

final class TableProcessorStreamPublisher<T> extends ListenerImpl implements StreamPublisher {
    private final ColumnSource<? extends T> source;
    private final ObjectProcessor<? super T> processor;
    private final int chunkSize;

    private StreamConsumer consumer;

    TableProcessorStreamPublisher(
            String description,
            Table parent,
            BaseTable<?> dependent,
            ColumnSource<? extends T> source,
            ObjectProcessor<? super T> processor,
            int chunkSize) {
        super(description, parent, dependent);
        this.source = Objects.requireNonNull(source);
        this.processor = Objects.requireNonNull(processor);
        this.chunkSize = chunkSize;
    }

    @Override
    public void onUpdate(TableUpdate upstream) {
        TableProcessorImpl.processAll(source, upstream.added(), false, processor, chunkSize, consumer);
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        this.consumer = Objects.requireNonNull(consumer);
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
