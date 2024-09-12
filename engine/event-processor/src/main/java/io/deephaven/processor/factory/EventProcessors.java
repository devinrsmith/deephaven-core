//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.factory;

import io.deephaven.processor.factory.EventProcessorFactory.EventProcessor;
import io.deephaven.processor.factory.EventProcessorFactory.EventProcessorSingle;
import io.deephaven.processor.sink.Stream;
import io.deephaven.processor.sink.appender.ObjectAppender;

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
        return new EventProcessorSingleConcat<>(x, y);
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

    private static class EventProcessorSingleConcat<T> implements EventProcessorSingle<T> {

        private final EventProcessorSingle<? super T> x;
        private final EventProcessorSingle<? super T> y;

        EventProcessorSingleConcat(EventProcessorSingle<? super T> x, EventProcessorSingle<? super T> y) {
            if (x.stream() != y.stream()) {
                throw new IllegalArgumentException();
            }
            this.x = x;
            this.y = y;
        }

        @Override
        public Stream stream() {
            return x.stream();
        }

        @Override
        public void setToSink(T event) {
            x.setToSink(event);
            y.setToSink(event);
        }

        @Override
        public void close() {
            // noinspection unused
            try (
                    final EventProcessorSingle<?> _x = x;
                    final EventProcessorSingle<?> _y = y) {
                // ignore
            }
        }
    }
}
