/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.stream;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;

public final class StreamPublisherIt implements StreamPublisher {
    private final List<Type<?>> columnTypes;
    private volatile boolean isShutdown;
    private StreamConsumer consumer;

    public StreamPublisherIt(List<Type<?>> columnTypes) {
        this.columnTypes = List.copyOf(columnTypes);
    }

    public void submit(Executor e, Iterator<List<WritableChunk<?>>> it, Runnable onItConsumed) {
        if (isShutdown) {
            if (onItConsumed != null) {
                onItConsumed.run();
            }
        } else {
            try {
                e.execute(new IteratorToConsumer(it, onItConsumed));
            } catch (Throwable t) {
                if (onItConsumed != null) {
                    onItConsumed.run();
                }
                throw t;
            }
        }
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
        isShutdown = true;
    }

    private final class IteratorToConsumer implements Runnable {
        private final Iterator<List<WritableChunk<?>>> it;
        private final Runnable onItConsumed;

        public IteratorToConsumer(Iterator<List<WritableChunk<?>>> it, Runnable onItConsumed) {
            this.it = Objects.requireNonNull(it);
            this.onItConsumed = onItConsumed;
        }

        @Override
        public void run() {
            try {
                runImpl();
            } catch (Throwable t) {
                consumer.acceptFailure(t);
            } finally {
                if (onItConsumed != null) {
                    onItConsumed.run();
                }
            }
        }

        private void runImpl() {
            // noinspection rawtypes
            final WritableChunk[] chunks = new WritableChunk[columnTypes.size()];
            while (!isShutdown && it.hasNext()) {
                final List<WritableChunk<?>> next = it.next();
                if (next.size() != chunks.length) {
                    throw new IllegalStateException(
                            String.format("Iterator returned inconsistent number of columns. expected=%d, actual=%d",
                                    chunks.length, next.size()));
                }
                next.toArray(chunks);
                // noinspection unchecked
                consumer.accept(chunks);
            }
        }
    }
}
