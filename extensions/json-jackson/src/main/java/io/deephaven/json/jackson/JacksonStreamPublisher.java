//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.json.Value;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BooleanSupplier;

public final class JacksonStreamPublisher implements StreamPublisher {

    public static JacksonStreamPublisher of(final JacksonIteratorProvider processorProvider) {
        return new JacksonStreamPublisher(processorProvider);
    }

    public static JacksonStreamPublisher stream(final Value options) {
        return of(JacksonIteratorProvider.stream(options));
    }

    public static JacksonStreamPublisher array(final Value options) {
        return of(JacksonIteratorProvider.array(options));
    }

    private static TableDefinition definition(final JacksonIteratorProvider processorProvider) {
        final List<String> names = processorProvider.names();
        final List<Type<?>> types = processorProvider.outputTypes();
        final int L = names.size();
        final List<ColumnDefinition<?>> cds = new ArrayList<>(L);
        for (int i = 0; i < L; ++i) {
            cds.add(ColumnDefinition.of(names.get(i), types.get(i)));
        }
        return TableDefinition.of(cds);
    }

    private final JacksonIteratorProvider processorProvider;
    private volatile boolean shutdown;
    private StreamConsumer consumer;

    JacksonStreamPublisher(final JacksonIteratorProvider processorProvider) {
        this.processorProvider = Objects.requireNonNull(processorProvider);
    }

    public TableDefinition definition() {
        return definition(processorProvider);
    }

    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public void register(@NotNull final StreamConsumer consumer) {
        if (this.consumer != null) {
            throw new IllegalStateException(String.format(
                    "Can not register multiple stream consumers: %s already registered, attempted to re-register %s",
                    this.consumer, consumer));
        }
        this.consumer = Objects.requireNonNull(consumer);
    }

    public boolean process(final JsonParser parser, final int bufferSize) throws IOException, InterruptedException {
        return process(parser, bufferSize, () -> true);
    }

    public boolean process(final JsonParser parser, final int bufferSize, final BooleanSupplier continueCondition)
            throws IOException, InterruptedException {
        final Thread currentThread = Thread.currentThread();
        final JacksonIterator it = processorProvider.iterator(parser, bufferSize);
        boolean hasNext;
        while ((hasNext = it.hasNext()) && !shutdown && !currentThread.isInterrupted()
                && continueCondition.getAsBoolean()) {
            accept(it.nextChunks());
        }
        if (hasNext) {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }
        return !hasNext;
    }

    // public boolean processUninterruptibly(final JsonParser parser, final int bufferSize) throws IOException {
    // return processUninterruptibly(parser, bufferSize, () -> true);
    // }
    //
    // public boolean processUninterruptibly(final JsonParser parser, final int bufferSize, final BooleanSupplier
    // continueCondition) throws IOException {
    // final JacksonIterator it = processorProvider.iterator(parser, bufferSize);
    // boolean hasNext;
    // while ((hasNext = it.hasNext()) && !shutdown && continueCondition.getAsBoolean()) {
    // accept(it.nextChunks());
    // }
    // return !hasNext;
    // }

    private void accept(final List<WritableChunk<?>> chunks) {
        // noinspection unchecked
        final WritableChunk<Values>[] array = chunks.toArray(new WritableChunk[0]);
        consumer.accept(array);
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
