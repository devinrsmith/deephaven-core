/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Objects;

final class IntChunkValueProcessor extends ValueProcessorBase {
    private final WritableIntChunk<?> chunk;

    IntChunkValueProcessor(String contextPrefix, boolean allowNull, boolean allowMissing, WritableIntChunk<?> chunk) {
        super(contextPrefix, allowNull, allowMissing);
        this.chunk = Objects.requireNonNull(chunk);
    }

    @Override
    public void handleValueNumberInt(JsonParser parser) throws IOException {
        chunk.add(parser.getIntValue());
    }

    @Override
    public void handleNull() {
        chunk.add(QueryConstants.NULL_INT);
    }

    @Override
    public void handleMissing() {
        chunk.add(QueryConstants.NULL_INT);
    }
}
