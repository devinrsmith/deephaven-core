/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ResettableObjectChunk;
import io.deephaven.chunk.ResettableWritableObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

final class ObjectProcessorFilteredImpl<T> implements ObjectProcessorFiltered<T> {
    private final ObjectProcessor<T> delegate;
    private final Predicate<T> filter;
    private final int rowLimit;

    ObjectProcessorFilteredImpl(ObjectProcessor<T> delegate, Predicate<T> filter, int rowLimit) {
        this.delegate = delegate;
        this.filter = filter;
        this.rowLimit = rowLimit;
    }

    @Override
    public List<Type<?>> outputTypes() {
        return delegate.outputTypes();
    }

    @Override
    public int processAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        // we have to have this blank wrapper function to hide the `? extends` capture since resetFromTypedChunk does
        // not support that form.
        return processAllImpl(in, out);
    }

    private <T2 extends T, ATTR extends Any> int processAllImpl(
            ObjectChunk<T2, ATTR> in,
            List<WritableChunk<?>> out) {
        int processed = 0;
        try (final WritableObjectChunk<T2, Any> chunk = WritableObjectChunk.makeWritableChunk(rowLimit)) {
            chunk.setSize(0);
            final int inSize = in.size();
            for (int i = 0; i < inSize; ++i) {
                final T2 item = in.get(i);
                if (!filter.test(item)) {
                    continue;
                }
                chunk.add(item);
                if (chunk.size() == rowLimit) {
                    delegate.processAll(chunk, out);
                    processed += rowLimit;
                    chunk.setSize(0);
                }
            }
            final int remaining = chunk.size();
            if (remaining != 0) {
                delegate.processAll(chunk, out);
                processed += remaining;
            }
            chunk.fillWithNullValue(0, Math.min(rowLimit, processed));
        }
        return processed;
    }
}
