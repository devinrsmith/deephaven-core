/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Default;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class DoubleOptions extends ValueOptions {
    public static Builder builder() {
        return null;
    }

    @Override
    @Default
    public boolean allowNull() {
        return true;
    }

    @Override
    @Default
    public boolean allowMissing() {
        return true;
    }

    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.of(Type.doubleType());
    }

    @Override
    final Map<JsonToken, JsonToken> startEndTokens() {
        return Map.of(
                JsonToken.VALUE_NUMBER_FLOAT, JsonToken.VALUE_NUMBER_FLOAT,
                JsonToken.VALUE_NUMBER_INT, JsonToken.VALUE_NUMBER_INT);
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new DoubleChunkValueProcessor(context, allowNull(), allowMissing(), out.get(0).asWritableDoubleChunk());
    }



    public interface Builder extends ValueOptions.Builder<DoubleOptions, Builder> {

    }
}
