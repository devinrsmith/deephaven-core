/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableDoubleChunk;

import java.io.IOException;
import java.util.Objects;

final class DoubleValueProcessor implements ValueProcessor {

    private final WritableDoubleChunk<?> out;
    private final ToDouble toDouble;

    DoubleValueProcessor(WritableDoubleChunk<?> out, ToDouble toDouble) {
        this.out = Objects.requireNonNull(out);
        this.toDouble = Objects.requireNonNull(toDouble);
    }

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        out.add(toDouble.parseValue(parser));
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        out.add(toDouble.parseMissing(parser));
    }
}