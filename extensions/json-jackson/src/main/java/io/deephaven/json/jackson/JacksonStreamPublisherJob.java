//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.iterators.ChunkedColumnIterator;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.util.annotations.VisibleForTesting;
import org.immutables.value.Value;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * An opinionated structuring around {@link JacksonStreamPublisher}.
 */
@Value.Immutable
@BuildableStyle
public abstract class JacksonStreamPublisherJob {


    public static State of(final JacksonValue2 spec, final Supplier<JsonParser> parserSupplier) {
        return builder()
                .iteratorSpec(spec)
                .addJobs(parserSupplier)
                .build()
                .state();
    }

    public static Builder builder() {
        return ImmutableJacksonStreamPublisherJob.builder();
    }

    /**
     * The iteration specification for
     * 
     * @return
     */
    public abstract JacksonValue2 iteratorSpec();

    public abstract List<Queue<? extends Supplier<JsonParser>>> jobs();

    /**
     * The name prefix. By default, is "JacksonStreamPublisherJob".
     */
    @Value.Default
    public String name() {
        return JacksonStreamPublisherJob.class.getSimpleName();
    }

    /**
     * The chunk capacity. By default, is {@value ChunkedColumnIterator#DEFAULT_CHUNK_SIZE}.
     */
    @Value.Default
    public int chunkCapacity() {
        return ChunkedColumnIterator.DEFAULT_CHUNK_SIZE;
    }

    /**
     * The update source registrar. By default, is {@code ExecutionContext.getContext().getUpdateGraph()}.
     *
     * @see ExecutionContext#getContext()
     * @see ExecutionContext#getUpdateGraph()
     */
    @Value.Default
    public UpdateSourceRegistrar registrar() {
        return ExecutionContext.getContext().getUpdateGraph();
    }

    /**
     * The executor. By default, is {@link ForkJoinPool#commonPool()}.
     */
    @Value.Default
    public Executor executor() {
        return ForkJoinPool.commonPool();
    }

    /**
     * Creates a state, composed of {@link JacksonStreamPublisher} and {@link StreamToBlinkTableAdapter}.
     * 
     * @return
     */
    public final State state() {
        return new State();
    }

    public interface Builder {

        Builder iteratorSpec(JacksonValue2 iteratorSpec);

        default Builder addSerialJobs(Supplier<JsonParser>... elements) {
            return addJobs(new LinkedList<>(Arrays.asList(elements)));
        }

        default Builder addParallelJobs(Supplier<JsonParser>... elements) {
            // noinspection unchecked
            return addJobs(Arrays.stream(elements).map(List::of).map(LinkedList::new).toArray(Queue[]::new));
        }

        default Builder addJobs(Supplier<JsonParser> element) {
            return addJobs(new LinkedList<>(List.of(element)));
        }

        Builder addJobs(Queue<? extends Supplier<JsonParser>> element);

        Builder addJobs(Queue<? extends Supplier<JsonParser>>... elements);

        Builder addAllJobs(Iterable<? extends Queue<? extends Supplier<JsonParser>>> elements);

        Builder name(String name);

        Builder chunkCapacity(int chunkCapacity);

        Builder registrar(UpdateSourceRegistrar registrar);

        Builder executor(Executor executor);

        JacksonStreamPublisherJob build();
    }

    public final class State {
        private final JacksonStreamPublisher publisher;
        private final StreamToBlinkTableAdapter adapter;
        private final AtomicBoolean continueCondition;
        private final CountDownLatch latch;
        private final AtomicBoolean started;

        State() {
            this.publisher = new JacksonStreamPublisher(iteratorSpec());
            this.adapter = new StreamToBlinkTableAdapter(
                    publisher.definition(),
                    publisher,
                    registrar(),
                    name() + "-" + UUID.randomUUID(),
                    Map.of(),
                    false);
            continueCondition = new AtomicBoolean(true);
            latch = new CountDownLatch(jobs().size());
            started = new AtomicBoolean(false);
        }

        /**
         * The {@link StreamToBlinkTableAdapter#table()}.
         */
        public Table table() {
            return adapter.table();
        }

        public void start() {
            if (!started.compareAndSet(false, true)) {
                return;
            }
            adapter.initialize();
            IntStream.range(0, jobs().size())
                    .mapToObj(ProcessJob::new)
                    .forEach(ProcessJob::execute);
        }

        public void cancel() {
            if (!continueCondition.compareAndSet(true, false)) {
                return;
            }
            publisher.acceptFailure(new CancellationException("User cancelled"));
        }

        public boolean isDone() {
            return latch.getCount() == 0;
        }

        public void await() throws InterruptedException {
            latch.await();
        }

        public boolean await(Duration duration) throws InterruptedException {
            return latch.await(duration.toNanos(), TimeUnit.NANOSECONDS);
        }

        @VisibleForTesting
        void runAdapter() {
            adapter.run();
        }

        private class ProcessJob implements Runnable {
            private final int ix;

            ProcessJob(int ix) {
                this.ix = ix;
            }

            private void execute() {
                executor().execute(this);
            }

            @Override
            public void run() {
                try {
                    runImpl();
                } finally {
                    latch.countDown();
                }
            }

            private void runImpl() {
                final Queue<? extends Supplier<JsonParser>> queue = jobs().get(ix);
                Supplier<JsonParser> parserSupplier;
                while ((parserSupplier = queue.poll()) != null) {
                    try (final JsonParser parser = parserSupplier.get()) {
                        parser.nextToken();
                        if (!publisher.process(parser, chunkCapacity(), continueCondition::get)) {
                            // Iterator not exhausted, means that shutdown has been invoked on the publisher (ie, the
                            // resulting table is no longer needed), or that continueCondition is false (either b/c
                            // there was an error on processing on another thread, or the user explicitly cancelled).
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
}
