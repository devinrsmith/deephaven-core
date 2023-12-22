/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableLongChunk;

import java.io.IOException;
import java.util.Objects;

abstract class LongChunkBase extends ValueProcessorBase {
    protected final WritableLongChunk<?> chunk;
    private final long onNull;
    private final long onMissing;

    LongChunkBase(
            String contextPrefix,
            boolean allowNull,
            boolean allowMissing,
            WritableLongChunk<?> chunk,
            long onNull,
            long onMissing) {
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
