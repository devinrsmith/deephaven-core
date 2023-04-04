package io.deephaven.engine.table.impl.sources.what;

import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

final class TableUpdatePublisher implements StreamPublisher {

    private static final TableDefinition DEFINITION = TableDefinition.of(ColumnDefinition.of("TableUpdate", Type.ofCustom(TableUpdate.class)));

    public static TableDefinition definition() {
        return DEFINITION;
    }

    private WritableObjectChunk<TableUpdate, Values> chunk;
    private StreamConsumer consumer;

    public TableUpdatePublisher() {
        chunk = WritableObjectChunk.makeWritableChunk(1024);
        chunk.setSize(0);
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        if (this.consumer != null) {
            throw new IllegalStateException("Can not register multiple StreamConsumers.");
        }
        this.consumer = Objects.requireNonNull(consumer);
    }

    public synchronized void add(TableUpdate tableUpdate) {
        chunk.add(tableUpdate);
    }

    @Override
    public synchronized void flush() {
        if (chunk.size() == 0) {
            return;
        }
        flushInternal();
    }

    private void flushInternal() {
        //noinspection unchecked
        consumer.accept(chunk);
        // todo: are we supposed to release?
        chunk = WritableObjectChunk.makeWritableChunk(1024);
        chunk.setSize(0);
    }

    public void acceptFailure(Throwable e) {
        consumer.acceptFailure(e);
    }
}
