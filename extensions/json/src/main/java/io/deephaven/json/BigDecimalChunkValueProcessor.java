/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableObjectChunk;

import java.io.IOException;
import java.math.BigDecimal;

final class BigDecimalChunkValueProcessor extends ObjectChunkValueProcessorBase<BigDecimal> {

    public BigDecimalChunkValueProcessor(String contextPrefix, boolean allowNull, boolean allowMissing,
            WritableObjectChunk<BigDecimal, ?> chunk) {
        super(contextPrefix, allowNull, allowMissing, chunk);
    }

    @Override
    protected void handleValueNumberInt(JsonParser parser) throws IOException {
        chunk.add(parser.getDecimalValue());
    }

    @Override
    protected void handleValueNumberFloat(JsonParser parser) throws IOException {
        chunk.add(parser.getDecimalValue());
    }
}