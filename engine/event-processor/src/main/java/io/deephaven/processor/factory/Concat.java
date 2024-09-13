//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.factory;

import io.deephaven.processor.sink.Coordinator;
import io.deephaven.processor.sink.Coordinators;
import io.deephaven.processor.sink.Sink;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class Concat<T> implements EventProcessorFactory<T> {

    public static <T> EventProcessorFactory<T> of(
            EventProcessorFactory<? super T> factory1,
            EventProcessorFactory<? super T> factory2,
            boolean useCoordinator) {
        return new Concat<>(factory1, factory2,
                false); // useCoordinator && (factory1.spec().usesCoordinator() || factory2.spec().usesCoordinator()));
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

    // @Override
    // public EventProcessorSpec spec() {
    // return EventProcessorSpec.builder()
    // .usesCoordinator(useCoordinator)
    // .addAllStreams(factory1.specs())
    // .addAllStreams(factory2.specs())
    // .build();
    // }

    @Override
    public List<EventProcessorStreamSpec> specs() {
        return Stream.concat(factory1.specs().stream(), factory2.specs().stream()).collect(Collectors.toList());
    }

    @Override
    public EventProcessor<T> create(Sink sink) {
        final Coordinator coordinator = useCoordinator
                ? sink.coordinator()
                : Coordinators.noop();
        final Sink sink1;
        {
            final Sink.Builder s1 = Sink.builder().coordinator(coordinator);
            for (EventProcessorStreamSpec e : factory1.specs()) {
                s1.putStreams(e.key(), Sink.get(sink, e.key()));
            }
            sink1 = s1.build();
        }
        final Sink sink2;
        {
            final Sink.Builder s2 = Sink.builder().coordinator(coordinator);
            for (EventProcessorStreamSpec e : factory2.specs()) {
                s2.putStreams(e.key(), Sink.get(sink, e.key()));
            }
            sink2 = s2.build();
        }
        final EventProcessor<? super T> p1 = factory1.create(sink1);
        try {
            final EventProcessor<? super T> p2 = factory2.create(sink2);
            try {
                return EventProcessors.concat(p1, p2);
            } catch (Throwable e) {
                p2.close();
                throw e;
            }
        } catch (Throwable e) {
            p1.close();
            throw e;
        }
    }
}
