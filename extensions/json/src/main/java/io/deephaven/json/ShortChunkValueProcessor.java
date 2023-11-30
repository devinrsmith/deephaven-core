/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Objects;

final class ShortChunkValueProcessor extends ValueProcessorBase {
    private final WritableShortChunk<?> chunk;

    ShortChunkValueProcessor(String contextPrefix, boolean allowNull, boolean allowMissing,
            WritableShortChunk<?> chunk) {
        super(contextPrefix, allowNull, allowMissing);
        this.chunk = Objects.requireNonNull(chunk);
    }

    @Override
    public void handleValueNumberInt(JsonParser parser) throws IOException {
        chunk.add(parser.getShortValue());
    }

    @Override
    public void handleNull() {
        chunk.add(QueryConstants.NULL_SHORT);
    }

    @Override
    public void handleMissing() {
        chunk.add(QueryConstants.NULL_SHORT);
    }
}
