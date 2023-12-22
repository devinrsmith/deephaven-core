/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableObjectChunk;

import java.io.IOException;
import java.util.Objects;

abstract class ArrayObjectChunkValueProcessorBase<A, V extends ValueProcessor> extends ArrayValueProcessorBase<V> {

    protected final WritableObjectChunk<A, ?> chunk;

    public ArrayObjectChunkValueProcessorBase(String contextPrefix, boolean allowNull, boolean allowMissing,
            WritableObjectChunk<A, ?> chunk) {
        super(contextPrefix, allowNull, allowMissing);
        this.chunk = Objects.requireNonNull(chunk);
    }

    @Override
    protected final void handleNull(JsonParser parser) throws IOException {
        chunk.add(null);
    }

    @Override
    protected final void handleMissing(JsonParser parser) throws IOException {
        chunk.add(null);
    }
}
