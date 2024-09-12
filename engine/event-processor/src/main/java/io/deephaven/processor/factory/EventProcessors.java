package io.deephaven.processor.factory;

import io.deephaven.processor.factory.EventProcessorFactory.EventProcessor;
import io.deephaven.processor.factory.EventProcessorFactory.EventProcessorSingle;
import io.deephaven.processor.sink.Stream;

import java.util.function.Consumer;

public final class EventProcessors {

    public static <T> EventProcessor<T> noClose(Consumer<T> consumer) {
        return new EventProcessor<>() {
            @Override
            public void writeToSink(T event) {
                consumer.accept(event);
            }

            @Override
            public void close() {

            }
        };
    }

    public static <T> EventProcessorSingle<T> concat(EventProcessorSingle<T> x, EventProcessorSingle<T> y) {
        final Stream stream = x.stream();
        if (stream != y.stream()) {
            throw new IllegalArgumentException();
        }
        return new EventProcessorSingle<>() {
            @Override
            public Stream stream() {
                return stream;
            }

            @Override
            public void setToSink(T event) {
                x.setToSink(event);
                y.setToSink(event);
            }

            @Override
            public void close() {
                //noinspection unused
                try (
                        final EventProcessorSingle<T> _x = x;
                        final EventProcessorSingle<T> _y = y) {
                    // ignore
                }
            }
        };
    }
}
