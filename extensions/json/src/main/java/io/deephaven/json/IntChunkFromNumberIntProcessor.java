/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.json.Functions.ToInt;

import java.io.IOException;
import java.util.Objects;

final class IntChunkFromNumberIntProcessor extends IntChunkBase {
    private final ToInt f;

    IntChunkFromNumberIntProcessor(
            String contextPrefix,
            boolean allowNull,
            boolean allowMissing,
            WritableIntChunk<?> chunk,
            int onNull, int onMissing, ToInt f) {
        super(contextPrefix, allowNull, allowMissing, chunk, onNull, onMissing);
        this.f = Objects.requireNonNull(f);
    }

    @Override
    protected void handleValueNumberInt(JsonParser parser) throws IOException {
        chunk.add(f.applyAsInt(parser));
    }
}
