/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableObjectChunk;

import java.io.IOException;
import java.util.Objects;

final class ObjectValueProcessor<T> implements ValueProcessor {

    private final WritableObjectChunk<T, ?> out;
    private final ToObject<T> toObj;

    ObjectValueProcessor(WritableObjectChunk<T, ?> out, ToObject<T> toObj) {
        this.out = Objects.requireNonNull(out);
        this.toObj = Objects.requireNonNull(toObj);
    }

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        out.add(toObj.parseValue(parser));
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        out.add(toObj.parseMissing(parser));
    }

    interface ToObject<T> {

        T parseValue(JsonParser parser) throws IOException;

        T parseMissing(JsonParser parser) throws IOException;
    }
}
