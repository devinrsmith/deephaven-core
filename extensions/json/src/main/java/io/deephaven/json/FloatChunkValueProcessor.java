/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Objects;

final class FloatChunkValueProcessor extends ValueProcessorBase {

    private final WritableFloatChunk<?> chunk;

    FloatChunkValueProcessor(String contextPrefix, boolean allowNull, boolean allowMissing,
            WritableFloatChunk<?> chunk) {
        super(contextPrefix, allowNull, allowMissing);
        this.chunk = Objects.requireNonNull(chunk);
    }

    @Override
    protected void handleValueNumberInt(JsonParser parser) throws IOException {
        chunk.add(parser.getFloatValue());
    }

    @Override
    public void handleValueNumberFloat(JsonParser parser) throws IOException {
        chunk.add(parser.getFloatValue());
    }

    @Override
    public void handleNull() {
        chunk.add(QueryConstants.NULL_FLOAT);
    }

    @Override
    public void handleMissing() {
        chunk.add(QueryConstants.NULL_FLOAT);
    }
}
