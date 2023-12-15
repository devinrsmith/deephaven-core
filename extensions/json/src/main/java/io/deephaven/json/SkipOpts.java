/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

// no builder
final class SkipOpts extends ValueOptions {

    private static final Map<JsonToken, JsonToken> TOKENS = Map.of(
            JsonToken.START_OBJECT, JsonToken.END_OBJECT,
            JsonToken.START_ARRAY, JsonToken.END_ARRAY,
            JsonToken.VALUE_STRING, JsonToken.VALUE_STRING,
            JsonToken.VALUE_NUMBER_INT, JsonToken.VALUE_NUMBER_INT,
            JsonToken.VALUE_NUMBER_FLOAT, JsonToken.VALUE_NUMBER_FLOAT,
            JsonToken.VALUE_TRUE, JsonToken.VALUE_TRUE,
            JsonToken.VALUE_FALSE, JsonToken.VALUE_FALSE,
            JsonToken.VALUE_NULL, JsonToken.VALUE_NULL);

    @Override
    public boolean allowNull() {
        return true;
    }

    @Override
    public boolean allowMissing() {
        return true;
    }

    @Override
    ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return ValueProcessor.skip();
    }

    @Override
    Map<JsonToken, JsonToken> startEndTokens() {
        return TOKENS;
    }

    @Override
    Stream<Type<?>> outputTypes() {
        return Stream.empty();
    }
}
