/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.Functions.ToDouble.Plain;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class DoubleOptions extends ValueOptions {

    public static DoubleOptions of() {
        return builder().build();
    }

    public static DoubleOptions ofStrict() {
        return builder().allowNull(false).allowMissing(false).build();
    }

    public static Builder builder() {
        return ImmutableDoubleOptions.builder();
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

    public abstract OptionalDouble onNull();

    public abstract OptionalDouble onMissing();

    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.of(Type.doubleType());
    }

    @Override
    final Set<JsonToken> startTokens() {
        return Set.of(JsonToken.VALUE_NUMBER_FLOAT, JsonToken.VALUE_NUMBER_INT);
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new DoubleChunkFromNumberFloatProcessor(context, allowNull(), allowMissing(),
                out.get(0).asWritableDoubleChunk(), onNull().orElse(QueryConstants.NULL_DOUBLE),
                onMissing().orElse(QueryConstants.NULL_DOUBLE), Plain.DOUBLE_VALUE);
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


    public interface Builder extends ValueOptions.Builder<DoubleOptions, Builder> {

        Builder onNull(double onNull);

        Builder onMissing(double onMissing);
    }
}
