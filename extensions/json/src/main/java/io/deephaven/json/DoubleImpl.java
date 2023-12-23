/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.json.Function.ToDouble;

import java.io.IOException;
import java.util.Objects;

final class DoubleImpl implements ValueProcessor {

    private final WritableDoubleChunk<?> out;
    private final ToDouble onValue;
    private final boolean allowMissing;
    private final double onMissing;

    DoubleImpl(WritableDoubleChunk<?> out, ToDouble onValue, boolean allowMissing, double onMissing) {
        this.out = Objects.requireNonNull(out);
        this.onValue = Objects.requireNonNull(onValue);
        this.allowMissing = allowMissing;
        this.onMissing = onMissing;
    }

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        out.add(onValue.parseValue(parser));
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        if (!allowMissing) {
            throw MismatchedInputException.from(parser, double.class,
                    String.format("Missing token, and allowMissing=false, %s", this));
        }
        out.add(onMissing);
    }
}
