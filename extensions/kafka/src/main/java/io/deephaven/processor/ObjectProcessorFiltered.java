/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.function.Predicate;

public interface ObjectProcessorFiltered<T> {

    static <T> ObjectProcessorFiltered<T> empty(List<Type<?>> outputTypes) {
        return new ObjectProcessorFilteredEmpty<>(outputTypes);
    }

    static <T> ObjectProcessorFiltered<T> of(ObjectProcessor<T> delegate) {
        return new ObjectProcessorFilteredAdapter<>(delegate);
    }

    static <T> ObjectProcessorFiltered<T> of(ObjectProcessor<T> delegate, Predicate<T> predicate, int rowLimit) {
        return new ObjectProcessorFilteredImpl<>(delegate, predicate, rowLimit);
    }

    List<Type<?>> outputTypes();

    int processAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out);
}
