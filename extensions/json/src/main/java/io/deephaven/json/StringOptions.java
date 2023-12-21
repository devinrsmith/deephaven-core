/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.Functions.ToObject.Plain;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class StringOptions extends ValueOptions {

    public static StringOptions of() {
        return builder().build();
    }

    public static Builder builder() {
        return ImmutableStringOptions.builder();
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

    public abstract Optional<String> onNull();

    public abstract Optional<String> onMissing();

    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.of(Type.stringType());
    }

    @Override
    final Set<JsonToken> startTokens() {
        return Set.of(JsonToken.VALUE_STRING);
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new ObjectChunkFromStringProcessor<>(context, allowMissing(), allowMissing(),
                out.get(0).asWritableObjectChunk(), onNull().orElse(null), onMissing().orElse(null),
                Plain.STRING_VALUE);
    }

    @Check
    final void checkOnNull() {
        if (!allowNull() && onNull().isPresent()) {
            throw new IllegalArgumentException();
        }
    }

    @Check
    final void checkOnMissing() {
        if (!allowMissing() && onMissing().isPresent()) {
            throw new IllegalArgumentException();
        }
    }

    public interface Builder extends ValueOptions.Builder<StringOptions, Builder> {

    }
}
