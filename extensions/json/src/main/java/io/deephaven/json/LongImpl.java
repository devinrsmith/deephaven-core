/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.json.Function.ToLong;

import java.io.IOException;
import java.util.Objects;

final class LongImpl implements ValueProcessor {

    private final WritableLongChunk<?> out;
    private final ToLong onValue;
    private final boolean allowMissing;
    private final long onMissing;

    LongImpl(WritableLongChunk<?> out, ToLong onValue, boolean allowMissing, long onMissing) {
        this.out = Objects.requireNonNull(out);
        this.onValue = Objects.requireNonNull(onValue);
        this.allowMissing = allowMissing;
        this.onMissing = onMissing;
    }

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        out.add(onValue.applyAsLong(parser));
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        if (!allowMissing) {
            throw MismatchedInputException.from(parser, Long.class,
                    String.format("Missing token, and allowMissing=false, %s", this));
        }
        out.add(onMissing);
    }
}
