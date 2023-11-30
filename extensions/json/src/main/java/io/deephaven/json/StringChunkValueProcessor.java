/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableObjectChunk;

import java.io.IOException;

final class StringChunkValueProcessor extends ObjectChunkValueProcessorBase<String> {
    public StringChunkValueProcessor(String contextPrefix, boolean allowNull, boolean allowMissing,
            WritableObjectChunk<String, ?> chunk) {
        super(contextPrefix, allowNull, allowMissing, chunk);
    }

    @Override
    protected void handleValueString(JsonParser parser) throws IOException {
        chunk.add(parser.getText());
    }
}
