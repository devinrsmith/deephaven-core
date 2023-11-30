/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.chunk.WritableObjectChunk;

import java.util.Objects;

abstract class ObjectChunkValueProcessorBase<T> extends ValueProcessorBase {
    protected final WritableObjectChunk<T, ?> chunk;

    ObjectChunkValueProcessorBase(String contextPrefix, boolean allowNull, boolean allowMissing,
            WritableObjectChunk<T, ?> chunk) {
        super(contextPrefix, allowNull, allowMissing);
        this.chunk = Objects.requireNonNull(chunk);
    }

    @Override
    public final void handleNull() {
        chunk.add(null);
    }

    @Override
    public final void handleMissing() {
        chunk.add(null);
    }
}
