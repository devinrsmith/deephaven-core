/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableIntChunk;

import java.io.IOException;
import java.util.Objects;

abstract class IntChunkBase extends ValueProcessorBase {
    protected final WritableIntChunk<?> chunk;
    private final int onNull;
    private final int onMissing;

    IntChunkBase(
            String contextPrefix,
            boolean allowNull,
            boolean allowMissing,
            WritableIntChunk<?> chunk,
            int onNull,
            int onMissing) {
        super(contextPrefix, allowNull, allowMissing);
        this.chunk = Objects.requireNonNull(chunk);
        this.onNull = onNull;
        this.onMissing = onMissing;
    }

    @Override
    public void handleNull(JsonParser parser) throws IOException {
        chunk.add(onNull);
    }

    @Override
    public void handleMissing(JsonParser parser) throws IOException {
        chunk.add(onMissing);
    }
}
