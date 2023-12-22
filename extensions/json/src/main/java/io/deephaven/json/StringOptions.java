/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.Function.ToObject;
import io.deephaven.json.Function.ToObject.Plain;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class StringOptions extends SingleValueOptions<String, ToObject<String>> {

    public static StringOptions of() {
        return builder().build();
    }

    public static Builder builder() {
        return null;
        // return ImmutableStringOptions.builder();
    }

    @Override
    @Default
    public boolean allowMissing() {
        return true;
    }

    public abstract Optional<String> onNull();


    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.of(Type.stringType());
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return null;
        // return new ObjectChunkFromStringProcessor<>(context, allowMissing(), allowMissing(),
        // out.get(0).asWritableObjectChunk(), onNull().orElse(null), onMissing().orElse(null),
        // Plain.STRING_VALUE);
    }

    @Check
    final void checkOnNull() {
        // if (!allowNull() && onNull().isPresent()) {
        // throw new IllegalArgumentException();
        // }
    }

    @Check
    final void checkOnMissing() {
        // if (!allowMissing() && onMissing().isPresent()) {
        // throw new IllegalArgumentException();
        // }
    }

    public interface Builder extends SingleValueOptions.Builder<String, ToObject<String>, StringOptions, Builder> {

    }
}
