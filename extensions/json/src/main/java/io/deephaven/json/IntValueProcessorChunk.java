/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableIntChunk;

import java.io.IOException;
import java.util.Objects;

final class IntValueProcessorChunk extends ValueProcessorBase {

    private final WritableIntChunk<?> chunk;
    private final int onNull;
    private final int onMissing;

    IntValueProcessorChunk(String contextPrefix, boolean allowNull, boolean allowMissing, WritableIntChunk<?> chunk, int onNull, int onMissing) {
        super(contextPrefix, allowNull, allowMissing);
        this.chunk = Objects.requireNonNull(chunk);
        this.onNull = onNull;
        this.onMissing = onMissing;
    }

    @Override
    public void handleValueNumberInt(JsonParser parser) throws IOException {
        chunk.add(parser.getIntValue());
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
