/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.util.List;

enum ObjectProcessorEmpty implements ObjectProcessor<Object> {
    OBJECT_PROCESSOR_EMPTY;

    public static <T> ObjectProcessor<T> of() {
        // noinspection unchecked
        return (ObjectProcessor<T>) OBJECT_PROCESSOR_EMPTY;
    }

    @Override
    public List<Type<?>> outputTypes() {
        return List.of();
    }

    @Override
    public void processAll(ObjectChunk<?, ?> in, List<WritableChunk<?>> out) {

    }
}
