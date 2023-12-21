/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

final class ObjectProcessorMap<T, R> implements ObjectProcessor<T> {
    private final Function<? super T, ? extends R> f;
    private final ObjectProcessor<? super R> delegate;

    ObjectProcessorMap(Function<? super T, ? extends R> f, ObjectProcessor<? super R> delegate) {
        this.f = Objects.requireNonNull(f);
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public List<Type<?>> outputTypes() {
        return delegate.outputTypes();
    }

    @Override
    public void processAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        try (final WritableObjectChunk<R, ?> mappedChunk = map(in, f)) {
            delegate.processAll(mappedChunk, out);
        }
    }

    private static <T, R, ATTR extends Any> WritableObjectChunk<R, ATTR> map(
            ObjectChunk<? extends T, ?> in,
            Function<? super T, ? extends R> f) {
        final int size = in.size();
        final WritableObjectChunk<R, ATTR> mappedChunk = WritableObjectChunk.makeWritableChunk(size);
        try {
            copy(in, mappedChunk, f);
            // size already set in makeWritableChunk
            // mappedChunk.setSize(size);
        } catch (Throwable t) {
            try {
                mappedChunk.close();
            } catch (Throwable t2) {
                t.addSuppressed(t2);
            }
            throw t;
        }
        return mappedChunk;
    }

    private static <T, R> void copy(
            ObjectChunk<? extends T, ?> src,
            WritableObjectChunk<? super R, ?> dst,
            Function<? super T, ? extends R> f) {
        final int size = src.size();
        for (int i = 0; i < size; ++i) {
            dst.set(i, f.apply(src.get(i)));
        }
    }
}
