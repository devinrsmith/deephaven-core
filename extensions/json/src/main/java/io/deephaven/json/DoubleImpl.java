/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.json.Functions.ToDouble;

import java.io.IOException;
import java.util.Objects;

class DoubleImpl implements ValueProcessor {

    private final WritableDoubleChunk<?> out;
    private final ToDouble f;
    private final boolean allowMissing;
    private final double onMissing;

    public DoubleImpl(WritableDoubleChunk<?> out, ToDouble f, boolean allowMissing, double onMissing) {
        this.out = Objects.requireNonNull(out);
        this.f = Objects.requireNonNull(f);
        this.allowMissing = allowMissing;
        this.onMissing = onMissing;
    }

    @Override
    public final void processCurrentValue(JsonParser parser) throws IOException {
        out.add(f.applyAsDouble(parser));
    }

    @Override
    public final void processMissing() {
        if (!allowMissing) {
            throw new IllegalStateException(
                    String.format("[%s]: Unexpected missing value, allowMissing=false", "todo"));
        }
        out.add(onMissing);
    }
}
