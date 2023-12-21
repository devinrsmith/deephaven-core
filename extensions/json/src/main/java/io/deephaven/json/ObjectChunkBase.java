/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.chunk.WritableObjectChunk;

import java.util.Objects;

abstract class ObjectChunkBase<T> extends ValueProcessorBase {
    protected final WritableObjectChunk<? super T, ?> chunk;
    private final T onNull;
    private final T onMissing;

    ObjectChunkBase(String contextPrefix, boolean allowNull, boolean allowMissing,
            WritableObjectChunk<? super T, ?> chunk, T onNull, T onMissing) {
        super(contextPrefix, allowNull, allowMissing);
        this.chunk = Objects.requireNonNull(chunk);
        this.onNull = onNull;
        this.onMissing = onMissing;
    }

    @Override
    public final void handleNull() {
        chunk.add(onNull);
    }

    @Override
    public final void handleMissing() {
        chunk.add(onMissing);
    }
}
