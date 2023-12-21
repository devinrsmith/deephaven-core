/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.Functions.ToLong.Plain;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class LongOptions extends ValueOptions {

    public static LongOptions of() {
        return builder().build();
    }

    public static LongOptions ofStrict() {
        return builder().allowNull(false).allowMissing(false).build();
    }

    public static Builder builder() {
        return ImmutableLongOptions.builder();
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

    public abstract OptionalLong onNull();

    public abstract OptionalLong onMissing();

    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.of(Type.longType());
    }

    @Override
    final Set<JsonToken> startTokens() {
        return Set.of(JsonToken.VALUE_NUMBER_INT);
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new LongChunkFromNumberIntProcessor(context, allowNull(), allowMissing(),
                out.get(0).asWritableLongChunk(),
                onNull().orElse(QueryConstants.NULL_LONG), onMissing().orElse(QueryConstants.NULL_LONG),
                Plain.LONG_VALUE);
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

    public interface Builder extends ValueOptions.Builder<LongOptions, Builder> {

        Builder onNull(long onNull);

        Builder onMissing(long onMissing);
    }
}
