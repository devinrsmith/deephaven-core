//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.json.Value;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

public final class JacksonPublisher implements StreamPublisher {

    public static JacksonPublisher of(final JacksonIteratorProvider processorProvider) {
        return new JacksonPublisher(processorProvider);
    }

    public static JacksonPublisher stream(final Value options) {
        return of(JacksonIteratorProvider.stream(options));
    }

    public static JacksonPublisher array(final Value options) {
        return of(JacksonIteratorProvider.array(options));
    }

    public static Table what(
            final Executor executor,
            final Value options,
            final int bufferSize,
            final Supplier<JsonParser> parserSupplier) {
        final JacksonPublisher publisher = JacksonPublisher.stream(options);
        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(publisher.definition(), publisher,
                ExecutionContext.getContext().getUpdateGraph(), "test");
        executor.execute(() -> {
            try (final JsonParser parser = parserSupplier.get()) {
                parser.nextToken();
                publisher.process(parser, bufferSize);
            } catch (IOException | RuntimeException | InterruptedException e) {
                publisher.acceptFailure(e);
            }
        });
        return adapter.table();
    }

    private final JacksonIteratorProvider processorProvider;
    private volatile boolean shutdown;
    private StreamConsumer consumer;

    JacksonPublisher(final JacksonIteratorProvider processorProvider) {
        this.processorProvider = Objects.requireNonNull(processorProvider);
    }

    public TableDefinition definition() {

        processorProvider.columnTypes();

    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        this.consumer = Objects.requireNonNull(consumer);
    }

    public void process(final JsonParser parser, final int bufferSize) throws IOException, InterruptedException {
        final JacksonIterator it = processorProvider.iterator(parser, bufferSize);
        while (it.hasNext() && !shutdown) {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            final List<WritableChunk<?>> chunks = it.nextChunks();
            // noinspection unchecked
            final WritableChunk<Values>[] array = chunks.toArray(new WritableChunk[0]);
            consumer.accept(array);
        }
    }

    public void acceptFailure(final Throwable cause) {
        consumer.acceptFailure(cause);
    }

    @Override
    public void flush() {
        // no-op, we pass chunks directly to consumer
    }

    @Override
    public void shutdown() {
        shutdown = true;
    }
}
