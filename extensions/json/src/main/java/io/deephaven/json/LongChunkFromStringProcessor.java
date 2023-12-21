/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.json.Functions.ToLong;

import java.io.IOException;
import java.util.Objects;

final class LongChunkFromStringProcessor extends LongChunkBase {
    private final ToLong f;

    LongChunkFromStringProcessor(
            String contextPrefix,
            boolean allowNull,
            boolean allowMissing,
            WritableLongChunk<?> chunk,
            long onNull, long onMissing, ToLong f) {
        super(contextPrefix, allowNull, allowMissing, chunk, onNull, onMissing);
        this.f = Objects.requireNonNull(f);
    }

    @Override
    protected void handleValueString(JsonParser parser) throws IOException {
        chunk.add(f.apply(parser));
    }
}
