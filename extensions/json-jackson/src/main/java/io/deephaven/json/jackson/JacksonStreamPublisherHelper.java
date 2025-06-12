//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.stream.StreamToBlinkTableAdapter;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class JacksonStreamPublisherHelper {
    public static Table execute(
            final JacksonStreamPublisher publisher,
            final String name,
            final int bufferSize,
            final UpdateSourceRegistrar registrar,
            final Executor executor,
            final Collection<? extends Queue<? extends Supplier<JsonParser>>> suppliers) {
        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(
                publisher.definition(),
                publisher,
                registrar,
                name);
        final AtomicBoolean continueCondition = new AtomicBoolean(true);
        for (final Queue<? extends Supplier<JsonParser>> queue : suppliers) {
            executor.execute(new ProcessJob(publisher, bufferSize, queue, continueCondition));
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
        return execute(publisher, name, bufferSize, registrar, executor, copyOne(suppliers));
    }

    public static Table parallel(
            final JacksonStreamPublisher publisher,
            final String name,
            final int bufferSize,
            final UpdateSourceRegistrar registrar,
            final Executor executor,
            final Collection<? extends Supplier<JsonParser>> suppliers) {
        return execute(publisher, name, bufferSize, registrar, executor, pushOne(suppliers));
    }

    public static Table parallel(
            final JacksonStreamPublisher publisher,
            final String name,
            final int bufferSize,
            final UpdateSourceRegistrar registrar,
            final Executor executor,
            final Queue<? extends Supplier<JsonParser>> suppliers,
            final int numJobs) {
        return execute(publisher, name, bufferSize, registrar, executor, duplicate(suppliers, numJobs));
    }

    private static <T> List<Queue<T>> pushOne(Collection<T> items) {
        return items.stream().map(List::of).map(LinkedList::new).collect(Collectors.toList());
    }

    private static <T> List<Queue<T>> copyOne(Collection<T> items) {
        return List.of(new LinkedList<>(items));
    }

    private static <C> List<C> duplicate(C item, int count) {
        return IntStream.range(0, count).mapToObj(ix -> item).collect(Collectors.toList());
    }

    private static class ProcessJob implements Runnable {
        private final JacksonStreamPublisher publisher;
        private final int bufferSize;
        private final Queue<? extends Supplier<JsonParser>> queue;
        private final AtomicBoolean continueCondition;

        ProcessJob(
                final JacksonStreamPublisher publisher,
                final int bufferSize,
                final Queue<? extends Supplier<JsonParser>> queue,
                final AtomicBoolean continueCondition) {
            this.publisher = Objects.requireNonNull(publisher);
            this.bufferSize = bufferSize;
            this.queue = Objects.requireNonNull(queue);
            this.continueCondition = Objects.requireNonNull(continueCondition);
        }

        @Override
        public void run() {
            Supplier<JsonParser> supplier;
            while ((supplier = queue.poll()) != null) {
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
