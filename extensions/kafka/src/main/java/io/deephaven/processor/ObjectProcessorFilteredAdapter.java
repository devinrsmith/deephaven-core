/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.Objects;

final class ObjectProcessorFilteredAdapter<T> implements ObjectProcessorFiltered<T> {
    private final ObjectProcessor<T> delegate;

    ObjectProcessorFilteredAdapter(ObjectProcessor<T> delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public List<Type<?>> outputTypes() {
        return delegate.outputTypes();
    }

    @Override
    public int processAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        delegate.processAll(in, out);
        return in.size();
    }
}
