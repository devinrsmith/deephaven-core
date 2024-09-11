//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.stream.json;


import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.processor.factory.EventProcessorFactory.EventProcessor;
import io.deephaven.processor.sink.Coordinator;
import io.deephaven.processor.sink.Sink;
import io.deephaven.processor.sink.Stream;
import io.deephaven.processor.sink.appender.IntAppender;
import io.deephaven.processor.sink.appender.LongAppender;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.SingleBlinkCoordinator;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;

import static io.deephaven.stream.json.JsonEventStuff.intValue;
import static io.deephaven.stream.json.JsonEventStuff.startArray;

public class NestedChunkImpl implements EventProcessor<JsonParser>, StreamPublisher {

    private static final int CHUNK_SIZE = 1024;

    public static Table read() {
        final TableDefinition tableDefinition = TableDefinition.of(
                ColumnDefinition.ofLong("OuterIx"),
                ColumnDefinition.ofLong("InnerIx"),
                ColumnDefinition.ofInt("Value"));
        final Coordinator coord = new Coordinator() {
            // don't care about re-entrancy; but maybe fairness?
            // private final Lock lock = new ReentrantLock(true);
            // private final Lock lock = new ReentrantLock(false);
            private final Lock lock = new StampedLock().asWriteLock();

            @Override
            public void writing() {
                lock.lock();
            }

            @Override
            public void sync() {
                lock.unlock();
            }
        };
        final NestedChunkImpl nested = new NestedChunkImpl(coord);

        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(tableDefinition, nested,
                ExecutionContext.getContext().getUpdateGraph(), "test");
        final Thread thread = new Thread(() -> {
            try (final JsonParser parser = new JsonFactory().createParser(new File("/tmp/test.json"))) {
                parser.nextToken();
                coord.writing();
                nested.writeToSink(parser);
                coord.sync();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        thread.start();
        return adapter.table();
    }

    private final Coordinator coordinator;

    private int pos;
    private WritableLongChunk<?> outerIndex;
    private WritableLongChunk<?> innerIndex;
    private WritableIntChunk<?> value;
    private StreamConsumer consumer;

    public NestedChunkImpl(Coordinator coordinator) {
        this.coordinator = Objects.requireNonNull(coordinator);
        pos = 0;
        outerIndex = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
        innerIndex = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
        value = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
    }

    @Override
    public void writeToSink(JsonParser event) {
        try {
            writeToSinkJackson(event);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        // noop
    }

    private void writeToSinkJackson(JsonParser parser) throws IOException {
        startArray(parser);
        for (long outerIx = 0; parser.nextToken() != JsonToken.END_ARRAY; ++outerIx) {
            startArray(parser);
            for (long innerIx = 0; parser.nextToken() != JsonToken.END_ARRAY; ++innerIx) {
                startArray(parser);
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    // no yield, we want to only deliver inner arrays together
                    outerIndex.set(pos, outerIx);
                    innerIndex.set(pos, innerIx);
                    value.set(pos, intValue(parser));
                    ++pos;
                    if (pos == CHUNK_SIZE) {
                        flushImpl();
                    }
                }
                coordinator.yield();
            }
            // Don't need outer yield given inner yield
            // coordinator.yield();
        }
    }

    @Override
    public void flush() {
        coordinator.writing(); // todo: hack
        flushImpl();
        coordinator.sync(); // todo: hack
    }

    private void flushImpl() {
        if (pos == 0) {
            return;
        }
        outerIndex.setSize(pos);
        innerIndex.setSize(pos);
        value.setSize(pos);
        try {
            consumer.accept(new WritableChunk[] {outerIndex, innerIndex, value});
        } finally {
            pos = 0;
            outerIndex = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
            innerIndex = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
            value = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
        }
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void shutdown() {

    }
}
