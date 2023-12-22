/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableLongChunk;

import java.util.Objects;

abstract class DoubleChunkBase extends ValueProcessorBase {
    protected final WritableDoubleChunk<?> chunk;
    private final double onNull;
    private final double onMissing;

    DoubleChunkBase(
            String contextPrefix,
            boolean allowNull,
            boolean allowMissing,
            WritableDoubleChunk<?> chunk,
            double onNull,
            double onMissing) {
        super(contextPrefix, allowNull, allowMissing);
        this.chunk = Objects.requireNonNull(chunk);
        this.onNull = onNull;
        this.onMissing = onMissing;
    }

    @Override
    public void handleNull() {
        chunk.add(onNull);
    }

    @Override
    public void handleMissing() {
        chunk.add(onMissing);
    }
}
