//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.factory;

import io.deephaven.processor.sink.Sink;

import java.util.Objects;
import java.util.function.Function;

final class Apply<T, R> implements EventProcessorFactory<T> {
    private final Function<? super T, ? extends R> f;
    private final EventProcessorFactory<? super R> factory;

    Apply(
            Function<? super T, ? extends R> f,
            EventProcessorFactory<? super R> factory) {
        this.f = Objects.requireNonNull(f);
        this.factory = Objects.requireNonNull(factory);
    }

    @Override
    public EventProcessorSpec spec() {
        return factory.spec();
    }

    @Override
    public EventProcessor<T> create(Sink sink) {
        final EventProcessor<? super R> processor = factory.create(sink);
        try {
            return new EventProcessorApply(processor);
        } catch (Throwable t) {
            processor.close();
            throw t;
        }
    }

    private class EventProcessorApply implements EventProcessor<T> {
        private final EventProcessor<? super R> eventProcessor;

        public EventProcessorApply(EventProcessor<? super R> eventProcessor) {
            this.eventProcessor = Objects.requireNonNull(eventProcessor);
        }

        @Override
        public void writeToSink(T event) {
            eventProcessor.writeToSink(f.apply(event));
        }

        @Override
        public void close() {
            eventProcessor.close();
        }
    }
}
