/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.Objects;

final class ObjectProcessorFilteredEmpty<T> implements ObjectProcessorFiltered<T> {
    private final List<Type<?>> types;

    ObjectProcessorFilteredEmpty(List<Type<?>> types) {
        this.types = Objects.requireNonNull(types);
    }

    @Override
    public List<Type<?>> outputTypes() {
        return types;
    }

    @Override
    public int processAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        return 0;
    }
}
