/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Objects;

final class ByteChunkValueProcessor extends ValueProcessorBase {
    private final WritableByteChunk<?> chunk;

    ByteChunkValueProcessor(String contextPrefix, boolean allowNull, boolean allowMissing, WritableByteChunk<?> chunk) {
        super(contextPrefix, allowNull, allowMissing);
        this.chunk = Objects.requireNonNull(chunk);
    }

    @Override
    public void handleValueNumberInt(JsonParser parser) throws IOException {
        chunk.add(parser.getByteValue());
    }

    @Override
    public void handleNull(JsonParser parser) throws IOException {
        chunk.add(QueryConstants.NULL_BYTE);
    }

    @Override
    public void handleMissing(JsonParser parser) throws IOException {
        chunk.add(QueryConstants.NULL_BYTE);
    }
}
