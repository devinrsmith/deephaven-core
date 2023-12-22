/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

// no builder
final class SkipOpts extends ValueOptions {

    private static final Set<JsonToken> START_TOKENS =
            Set.of(JsonToken.START_OBJECT, JsonToken.START_ARRAY, JsonToken.VALUE_STRING, JsonToken.VALUE_NUMBER_INT,
                    JsonToken.VALUE_NUMBER_FLOAT, JsonToken.VALUE_TRUE, JsonToken.VALUE_FALSE, JsonToken.VALUE_NULL);

    @Override
    public boolean allowMissing() {
        return true;
    }

    @Override
    ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return ValueProcessor.skip();
    }

    @Override
    Stream<Type<?>> outputTypes() {
        return Stream.empty();
    }
}
