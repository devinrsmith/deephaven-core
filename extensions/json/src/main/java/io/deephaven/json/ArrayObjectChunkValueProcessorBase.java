/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.chunk.WritableObjectChunk;

import java.util.Objects;

abstract class ArrayObjectChunkValueProcessorBase<A, T extends ValueProcessor> extends ArrayValueProcessorBase<T> {

    protected final WritableObjectChunk<A, ?> chunk;

    public ArrayObjectChunkValueProcessorBase(String contextPrefix, boolean allowNull, boolean allowMissing, WritableObjectChunk<A, ?> chunk) {
        super(contextPrefix, allowNull, allowMissing);
        this.chunk = Objects.requireNonNull(chunk);
    }

    @Override
    protected final void handleNull() {
        chunk.add(null);
    }

    @Override
    protected final void handleMissing() {
        chunk.add(null);
    }
}
