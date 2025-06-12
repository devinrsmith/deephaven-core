//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.stream.StreamToBlinkTableAdapter;
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

@Value.Immutable
@BuildableStyle
public abstract class JacksonStreamPublisherJob {

    public static Builder builder() {
        return ImmutableJacksonStreamPublisherJob.builder();
    }

    public abstract JacksonIteratorSpec iteratorSpec();

    public abstract List<Queue<? extends Supplier<JsonParser>>> jobs();

    @Value.Default
    public String name() {
        return JacksonStreamPublisherJob.class.getSimpleName();
    }

    @Value.Default
    public int bufferSize() {
        return 4096; // todo: what is the best value?
    }

    @Value.Default
    public UpdateSourceRegistrar registrar() {
        return ExecutionContext.getContext().getUpdateGraph();
    }

    @Value.Default
    public Executor executor() {
        return ForkJoinPool.commonPool();
    }

    public final State state() {
        return new State();
    }

    public interface Builder {

        Builder iteratorSpec(JacksonIteratorSpec iteratorSpec);

        default Builder addSerialJobs(Supplier<JsonParser>... elements) {
            return addJobs(new LinkedList<>(Arrays.asList(elements)));
        }

        default Builder addParallelJobs(Supplier<JsonParser>... elements) {
            //noinspection unchecked
            return addJobs(Arrays.stream(elements).map(List::of).map(LinkedList::new).toArray(Queue[]::new));
        }

        default Builder addJobs(Supplier<JsonParser> element) {
            return addJobs(new LinkedList<>(List.of(element)));
        }

        Builder addJobs(Queue<? extends Supplier<JsonParser>> element);

        Builder addJobs(Queue<? extends Supplier<JsonParser>>... elements);

        Builder addAllJobs(Iterable<? extends Queue<? extends Supplier<JsonParser>>> elements);

        Builder name(String name);

        Builder bufferSize(int bufferSize);

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
                        if (!publisher.process(parser, bufferSize(), continueCondition::get)) {
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
