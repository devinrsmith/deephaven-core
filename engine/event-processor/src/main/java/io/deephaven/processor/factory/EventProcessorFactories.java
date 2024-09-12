//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.factory;

import io.deephaven.processor.factory.EventProcessorFactory.EventProcessor;
import io.deephaven.processor.sink.Sink;
import io.deephaven.processor.sink.Stream;
import io.deephaven.processor.sink.appender.ObjectAppender;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.GenericType;

import java.util.function.Function;

public final class EventProcessorFactories {
    public static <T, R> EventProcessorFactory<T> map(
            Function<? super T, ? extends R> f,
            EventProcessorFactory<? super R> factory) {
        return new Apply<T, R>(f, factory);
    }

    // todo: demonstrate a more advanced concat, that allows concat on demand, and if the factory fails, removes it.
    // this could be applicable for centralizing all kafka for the same topic into a single subscriber

    public static <T> EventProcessorFactory<T> concat(
            EventProcessorFactory<? super T> factory1,
            EventProcessorFactory<? super T> factory2,
            boolean useCoordinator) {
        return Concat.of(factory1, factory2, useCoordinator);
    }



    public static <T> EventProcessorFactory<T> of(EventProcessorSpec spec, Function<Sink, EventProcessor<T>> f) {
        return new EventProcessorFactory<T>() {
            @Override
            public EventProcessorSpec spec() {
                return spec;
            }

            @Override
            public EventProcessor<T> create(Sink sink) {
                return f.apply(sink);
            }
        };
    }

    public static <T> EventProcessorFactory<T> singleton(GenericType<T> type, EventProcessorSpec spec) {
        if (type instanceof BoxedType) {
            throw new UnsupportedOperationException(); // todo
        }
        return of(spec, sink -> singleton(type, sink));
    }

    private static <X> EventProcessor<X> singleton(GenericType<X> type, Sink sink) {
        if (type instanceof BoxedType) {
            throw new UnsupportedOperationException(); // todo
        }
        final Stream stream = sink.singleStream();
        if (stream.appenders().size() != 1) {
            throw new IllegalStateException();
        }
        final ObjectAppender<X> appender = ObjectAppender.get(stream.appenders().get(0), type);
        return EventProcessors.singleton(appender);
    }
}
