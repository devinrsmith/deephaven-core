package io.deephaven.stream;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.util.Objects;


/**
 * A {@link Table Table-based} consumption model that abstracts some of the implementation details of
 * {@link StreamToTableAdapter} and makes it easy to consume data via {@link TableStreamConsumer}.
 */
public final class TableToStream implements Runnable, SafeCloseable {

    public static TableToStream of(
            String name,
            TableDefinition definition,
            UpdateSourceRegistrar updateSourceRegistrar,
            @Nullable Runnable flushCallback,
            @Nullable Runnable shutdownCallback) {
        final PublisherImpl publisher = new PublisherImpl(flushCallback);
        final StreamToTableAdapter adapter =
                new StreamToTableAdapter(definition, publisher, updateSourceRegistrar, name);
        if (shutdownCallback != null) {
            adapter.setShutdownCallback(shutdownCallback);
        }
        return new TableToStream(publisher.consumer(), adapter);
    }

    private final StreamConsumer consumer;
    private final StreamToTableAdapter adapter;

    private TableToStream(StreamConsumer consumer, StreamToTableAdapter adapter) {
        this.consumer = Objects.requireNonNull(consumer);
        this.adapter = Objects.requireNonNull(adapter);
    }

    /**
     * Return the {@link Table#STREAM_TABLE_ATTRIBUTE stream} {@link Table table} that this adapter is producing, and
     * ensure that {@code this} no longer enforces strong reachability of the result. May return {@code null} if invoked
     * more than once.
     *
     * @return The resulting stream table
     */
    public Table table() {
        return adapter.table();
    }

    /**
     * Create a table stream consumer.
     *
     * @param chunkSize the chunk size
     * @param sync whether chunked additions to the consumer need to be synchronous
     * @return the consumer
     */
    public TableStreamConsumer consumer(int chunkSize, boolean sync) {
        return new TableStreamConsumer(consumer, adapter.getTableDefinition(), chunkSize, sync);
    }

    @Override
    public void run() {
        adapter.run();
    }

    @Override
    public void close() {
        adapter.close();
    }

    private static class PublisherImpl implements StreamPublisher {

        private final Runnable flushCallback;
        private StreamConsumer consumer;

        public PublisherImpl(Runnable flushCallback) {
            this.flushCallback = flushCallback;
        }

        public StreamConsumer consumer() {
            return Objects.requireNonNull(consumer);
        }

        @Override
        public void register(@NotNull StreamConsumer consumer) {
            if (this.consumer != null) {
                throw new IllegalStateException("Can not register multiple StreamConsumers.");
            }
            this.consumer = Objects.requireNonNull(consumer);
        }

        @Override
        public void flush() {
            if (flushCallback != null) {
                flushCallback.run();
            }
        }
    }
}
