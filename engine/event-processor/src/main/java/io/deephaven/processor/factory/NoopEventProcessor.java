//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.factory;

import io.deephaven.processor.sink.Sink;

import java.util.List;
import java.util.Objects;

public final class NoopEventProcessor<T> implements EventProcessorFactory<T> {

    public static <T> EventProcessorFactory<T> of(EventProcessorSpec spec) {
        return new NoopEventProcessor<>(spec);
    }

    private final EventProcessorSpec spec;

    private NoopEventProcessor(EventProcessorSpec spec) {
        this.spec = Objects.requireNonNull(spec);
    }

    // @Override
    // public EventProcessorSpec spec() {
    // return spec;
    // }

    @Override
    public List<EventProcessorStreamSpec> specs() {
        return List.of();
    }

    @Override
    public EventProcessor<T> create(Sink sink) {
        // noinspection unchecked
        return (EventProcessor<T>) Impl.NOOP;
    }

    enum Impl implements EventProcessor<Object> {
        NOOP;

        @Override
        public void writeToSink(Object event) {
            // ignore
        }

        @Override
        public void close() {
            // ignore
        }
    }
}
