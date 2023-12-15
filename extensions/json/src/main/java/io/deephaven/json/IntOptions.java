/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class IntOptions extends ValueOptions {

    public static IntOptions of() {
        return builder().build();
    }

    public static Builder builder() {
        return ImmutableIntOptions.builder();
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
        return Stream.of(Type.intType());
    }

    @Override
    final Map<JsonToken, JsonToken> startEndTokens() {
        return Map.of(JsonToken.VALUE_NUMBER_INT, JsonToken.VALUE_NUMBER_INT);
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new IntChunkValueProcessor(context, allowNull(), allowMissing(), out.get(0).asWritableIntChunk());
    }

    public interface Builder extends ValueOptions.Builder<IntOptions, Builder> {

    }
}
