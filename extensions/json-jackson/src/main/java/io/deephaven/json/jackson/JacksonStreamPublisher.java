//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.json.Value;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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

    public static Table execute(
            final JacksonStreamPublisher publisher,
            final String name,
            final int bufferSize,
            final UpdateSourceRegistrar registrar,
            final Executor executor,
            final Collection<? extends Collection<? extends Supplier<JsonParser>>> suppliers) {
        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(
                publisher.definition(),
                publisher,
                registrar,
                name);
        final AtomicBoolean continueCondition = new AtomicBoolean(true);
        for (final Collection<? extends Supplier<JsonParser>> s : suppliers) {
            executor.execute(new ProcessJob(publisher, bufferSize, s, continueCondition));
        }
        return adapter.table();
    }

    public static Table serial(
            final JacksonStreamPublisher publisher,
            final String name,
            final int bufferSize,
            final UpdateSourceRegistrar registrar,
            final Executor executor,
            final Collection<? extends Supplier<JsonParser>> suppliers) {
        return execute(publisher, name, bufferSize, registrar, executor, List.of(suppliers));
    }

    public static Table parallel(
            final JacksonStreamPublisher publisher,
            final String name,
            final int bufferSize,
            final UpdateSourceRegistrar registrar,
            final Executor executor,
            final Collection<? extends Supplier<JsonParser>> suppliers) {
        return execute(publisher, name, bufferSize, registrar, executor,
                suppliers.stream().map(List::of).collect(Collectors.toList()));
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
        final boolean iteratorExhausted = processorProvider
                .iterator(parser, bufferSize)
                .forEachRemaining(this::accept, this::loopConditionInterruptible);
        if (!iteratorExhausted) {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }
        return iteratorExhausted;
    }

    public boolean process(final JsonParser parser, final int bufferSize, final BooleanSupplier condition)
            throws IOException, InterruptedException {
        final boolean iteratorExhausted = processorProvider
                .iterator(parser, bufferSize)
                .forEachRemaining(this::accept, () -> loopConditionInterruptible() && condition.getAsBoolean());
        if (!iteratorExhausted) {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }
        return iteratorExhausted;
    }

    public boolean processUninterruptibly(final JsonParser parser, final int bufferSize) throws IOException {
        return processorProvider
                .iterator(parser, bufferSize)
                .forEachRemaining(this::accept, this::loopConditionUninterruptible);
    }

    public boolean processUninterruptibly(final JsonParser parser, final int bufferSize,
            final BooleanSupplier condition) throws IOException {
        return processorProvider
                .iterator(parser, bufferSize)
                .forEachRemaining(this::accept, () -> loopConditionUninterruptible() && condition.getAsBoolean());
    }

    private boolean loopConditionUninterruptible() {
        return !shutdown;
    }

    private boolean loopConditionInterruptible() {
        return !shutdown && !Thread.currentThread().isInterrupted();
    }

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

    private static class ProcessJob implements Runnable {
        private final JacksonStreamPublisher publisher;
        private final int bufferSize;
        private final Collection<? extends Supplier<JsonParser>> suppliers;
        private final AtomicBoolean continueCondition;

        ProcessJob(
                final JacksonStreamPublisher publisher,
                final int bufferSize,
                final Collection<? extends Supplier<JsonParser>> suppliers,
                final AtomicBoolean continueCondition) {
            this.publisher = Objects.requireNonNull(publisher);
            this.bufferSize = bufferSize;
            this.suppliers = Objects.requireNonNull(suppliers);
            this.continueCondition = Objects.requireNonNull(continueCondition);
        }

        @Override
        public void run() {
            for (final Supplier<JsonParser> supplier : suppliers) {
                try (final JsonParser parser = supplier.get()) {
                    parser.nextToken();
                    if (!publisher.process(parser, bufferSize, continueCondition::get)) {
                        return;
                    }
                } catch (final IOException | RuntimeException | InterruptedException e) {
                    if (continueCondition.compareAndSet(true, false)) {
                        publisher.acceptFailure(e);
                    }
                    return;
                }
            }
        }
    }
}
