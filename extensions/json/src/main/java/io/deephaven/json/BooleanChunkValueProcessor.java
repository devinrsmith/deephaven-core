/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.util.BooleanUtils;

import java.util.Objects;

final class BooleanChunkValueProcessor extends ValueProcessorBase {

    private final WritableByteChunk<?> chunk;

    BooleanChunkValueProcessor(String contextPrefix, boolean allowNull, boolean allowMissing, WritableByteChunk<?> chunk) {
        super(contextPrefix, allowNull, allowMissing);
        this.chunk = Objects.requireNonNull(chunk);
    }

    @Override
    protected void handleValueTrue(JsonParser parser) {
        chunk.add(BooleanUtils.TRUE_BOOLEAN_AS_BYTE);
    }

    @Override
    protected void handleValueFalse(JsonParser parser) {
        chunk.add(BooleanUtils.FALSE_BOOLEAN_AS_BYTE);
    }

    @Override
    public void handleNull() {
        chunk.add(BooleanUtils.NULL_BOOLEAN_AS_BYTE);
    }

    @Override
    public void handleMissing() {
        chunk.add(BooleanUtils.NULL_BOOLEAN_AS_BYTE);
    }
}
