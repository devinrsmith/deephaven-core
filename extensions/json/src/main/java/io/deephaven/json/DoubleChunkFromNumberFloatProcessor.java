/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.json.Function.ToDouble;

import java.io.IOException;
import java.util.Objects;

final class DoubleChunkFromNumberFloatProcessor extends DoubleChunkBase {
    private final ToDouble f;

    DoubleChunkFromNumberFloatProcessor(
            String contextPrefix,
            boolean allowNull,
            boolean allowMissing,
            WritableDoubleChunk<?> chunk,
            double onNull,
            double onMissing,
            ToDouble f) {
        super(contextPrefix, allowNull, allowMissing, chunk, onNull, onMissing);
        this.f = Objects.requireNonNull(f);
    }

    @Override
    protected void handleValueNumberFloat(JsonParser parser) throws IOException {
        chunk.add(f.parseValue(parser));
    }

    @Override
    protected void handleValueNumberInt(JsonParser parser) throws IOException {
        chunk.add(f.parseValue(parser));
    }
}
