//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.factory;

import io.deephaven.processor.sink.Sink;

import java.util.function.Consumer;
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
}
