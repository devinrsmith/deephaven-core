//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.factory;

import io.deephaven.processor.factory.EventProcessorFactory.EventProcessor;
import io.deephaven.processor.sink.appender.ObjectAppender;

import java.util.Objects;
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

    public static <T> EventProcessor<T> concat(EventProcessor<? super T> x, EventProcessor<? super T> y) {
        return new EventProcessorConcat<>(x, y);
    }

    // todo: primitive versions
    public static <T> EventProcessor<T> singleton(ObjectAppender<T> appender) {
        // todo: not row oriented
        return noClose(t -> {
            if (t == null) {
                ObjectAppender.appendNull(appender);
            } else {
                ObjectAppender.append(appender, t);
            }
        });
    }

    private static class EventProcessorConcat<T> implements EventProcessor<T> {

        private final EventProcessor<? super T> x;
        private final EventProcessor<? super T> y;

        EventProcessorConcat(EventProcessor<? super T> x, EventProcessor<? super T> y) {
            this.x = Objects.requireNonNull(x);
            this.y = Objects.requireNonNull(y);
        }

        @Override
        public void writeToSink(T event) {
            x.writeToSink(event);
            y.writeToSink(event);
        }

        @Override
        public void close() {
            // noinspection unused
            try (
                    final EventProcessor<?> _x = x;
                    final EventProcessor<?> _y = y) {
                // ignore
            }
        }
    }
}
