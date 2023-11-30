/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Objects;

final class LongChunkValueProcessor extends ValueProcessorBase {

    private final WritableLongChunk<?> chunk;

    LongChunkValueProcessor(String contextPrefix, boolean allowNull, boolean allowMissing, WritableLongChunk<?> chunk,
            long onNull, long onMissing) {
        super(contextPrefix, allowNull, allowMissing);
        this.chunk = Objects.requireNonNull(chunk);
    }

    @Override
    public void handleValueNumberInt(JsonParser parser) throws IOException {
        chunk.add(parser.getLongValue());
    }

    @Override
    public void handleNull() {
        chunk.add(QueryConstants.NULL_LONG);
    }

    @Override
    public void handleMissing() {
        chunk.add(QueryConstants.NULL_LONG);
    }
}
