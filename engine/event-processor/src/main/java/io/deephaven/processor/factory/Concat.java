//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.factory;

import io.deephaven.processor.sink.Coordinator;
import io.deephaven.processor.sink.Coordinators;
import io.deephaven.processor.sink.Sink;

import java.util.Objects;

final class Concat<T> implements EventProcessorFactory<T> {

    public static <T> EventProcessorFactory<T> of(
            EventProcessorFactory<? super T> factory1,
            EventProcessorFactory<? super T> factory2,
            boolean useCoordinator) {
        return new Concat<>(factory1, factory2,
                useCoordinator && (factory1.spec().usesCoordinator() || factory2.spec().usesCoordinator()));
    }

    private final EventProcessorFactory<? super T> factory1;
    private final EventProcessorFactory<? super T> factory2;
    private final boolean useCoordinator;

    private Concat(
            EventProcessorFactory<? super T> factory1,
            EventProcessorFactory<? super T> factory2,
            boolean useCoordinator) {
        this.factory1 = Objects.requireNonNull(factory1);
        this.factory2 = Objects.requireNonNull(factory2);
        this.useCoordinator = useCoordinator;
    }

    @Override
    public EventProcessorSpec spec() {
        return EventProcessorSpec.builder()
                .usesCoordinator(useCoordinator)
                .addAllStreams(factory1.spec().streams())
                .addAllStreams(factory2.spec().streams())
                .build();
    }

    @Override
    public EventProcessor<T> create(Sink sink) {
        final Coordinator coordinator = useCoordinator
                ? sink.coordinator()
                : Coordinators.noop();
        final int f1 = factory1.spec().numStreams();
        final int f2 = factory2.spec().numStreams();
        final Sink sink1 = Sink.builder()
                .coordinator(coordinator)
                .addAllStreams(sink.streams().subList(0, f1))
                .build();
        final Sink sink2 = Sink.builder()
                .coordinator(coordinator)
                .addAllStreams(sink.streams().subList(f1, f1 + f2))
                .build();
        final EventProcessor<? super T> p1 = factory1.create(sink1);
        try {
            final EventProcessor<? super T> p2 = factory2.create(sink2);
            try {
                return new CombinedEventProcessor<>(p1, p2);
            } catch (Throwable e) {
                p2.close();
                throw e;
            }
        } catch (Throwable e) {
            p1.close();
            throw e;
        }
    }

    private static class CombinedEventProcessor<T> implements EventProcessor<T> {
        private final EventProcessor<? super T> p1;
        private final EventProcessor<? super T> p2;

        public CombinedEventProcessor(EventProcessor<? super T> p1, EventProcessor<? super T> p2) {
            this.p1 = Objects.requireNonNull(p1);
            this.p2 = Objects.requireNonNull(p2);
        }

        @Override
        public void writeToSink(T event) {
            p1.writeToSink(event);
            p2.writeToSink(event);
        }

        @Override
        public void close() {
            // noinspection unused,EmptyTryBlock
            try (
                    final EventProcessor<?> _p1 = p1;
                    final EventProcessor<?> _p2 = p2) {
                // ignore
            }
        }
    }
}
