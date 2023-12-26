/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableIntChunk;

import java.io.IOException;
import java.util.Objects;

final class IntValueProcessor implements ValueProcessor {

    private final WritableIntChunk<?> out;
    private final ToInt toInt;

    IntValueProcessor(WritableIntChunk<?> out, ToInt toInt) {
        this.out = Objects.requireNonNull(out);
        this.toInt = Objects.requireNonNull(toInt);
    }

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        out.add(toInt.parseValue(parser));
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        out.add(toInt.parseMissing(parser));
    }
}