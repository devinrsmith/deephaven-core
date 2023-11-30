/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableObjectChunk;

import java.io.IOException;
import java.math.BigInteger;

final class BigIntegerChunkValueProcessor extends ObjectChunkValueProcessorBase<BigInteger> {

    public BigIntegerChunkValueProcessor(String contextPrefix, boolean allowNull, boolean allowMissing, WritableObjectChunk<BigInteger, ?> chunk, BigInteger onNull, BigInteger onMissing) {
        super(contextPrefix, allowNull, allowMissing, chunk);
    }

    @Override
    protected void handleValueNumberInt(JsonParser parser) throws IOException {
        chunk.add(parser.getBigIntegerValue());
    }
}
