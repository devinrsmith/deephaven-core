/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableLongChunk;

import java.io.IOException;
import java.util.Objects;

final class LongValueProcessor implements ValueProcessor {

    private final WritableLongChunk<?> out;
    private final ToLong toLong;

    LongValueProcessor(WritableLongChunk<?> out, ToLong toLong) {
        this.out = Objects.requireNonNull(out);
        this.toLong = Objects.requireNonNull(toLong);
    }

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        out.add(toLong.parseValue(parser));
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        out.add(toLong.parseMissing(parser));
    }

    interface ToLong {

        long parseValue(JsonParser parser) throws IOException;

        long parseMissing(JsonParser parser) throws IOException;
    }
}
