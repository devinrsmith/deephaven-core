/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.stream;

import io.deephaven.chunk.WritableChunk;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.List;

public class StreamPublisherWhat implements StreamPublisher {

    private final Iterator<List<WritableChunk<?>>> it;
    private StreamConsumer streamConsumer;

    public StreamPublisherWhat(Iterator<List<WritableChunk<?>>> it) {
        this.it = it;
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        this.streamConsumer = consumer;
    }

    @Override
    public void flush() {

    }

    @Override
    public void shutdown() {

    }
}
