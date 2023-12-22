/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Objects;

final class DoubleChunkValueProcessor extends ValueProcessorBase {
    private final WritableDoubleChunk<?> chunk;

    DoubleChunkValueProcessor(String contextPrefix, boolean allowNull, boolean allowMissing,
            WritableDoubleChunk<?> chunk) {
        super(contextPrefix, allowNull, allowMissing);
        this.chunk = Objects.requireNonNull(chunk);
    }

    @Override
    protected void handleValueNumberInt(JsonParser parser) throws IOException {
        chunk.add(parser.getDoubleValue());
    }

    @Override
    public void handleValueNumberFloat(JsonParser parser) throws IOException {
        chunk.add(parser.getDoubleValue());
    }

    @Override
    public void handleNull(JsonParser parser) throws IOException {
        chunk.add(QueryConstants.NULL_DOUBLE);
    }

    @Override
    public void handleMissing(JsonParser parser) throws IOException {
        chunk.add(QueryConstants.NULL_DOUBLE);
    }
}
