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
public abstract class TimestampOptions extends ValueOptions {

    public enum Format {
        ISO_8601, EPOCH_SECONDS, EPOCH_MILLIS, EPOCH_MICROS, EPOCH_NANOS
    }

    public static TimestampOptions of() {
        return builder().build();
    }

    public static Builder builder() {
        return ImmutableTimestampOptions.builder();
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

    @Default
    public Format format() {
        return Format.ISO_8601;
    }

    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.of(Type.instantType());
    }

    @Override
    final Map<JsonToken, JsonToken> startEndTokens() {
        return format() == Format.ISO_8601
                ? Map.of(JsonToken.VALUE_STRING, JsonToken.VALUE_STRING)
                : Map.of(JsonToken.VALUE_NUMBER_INT, JsonToken.VALUE_NUMBER_INT);
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        final Format f = format();
        return f == Format.ISO_8601
                ? new TimestampProcessor(context, allowNull(), allowMissing(), out.get(0).asWritableLongChunk())
                : new TimestampIntProcessor(context, allowNull(), allowMissing(), out.get(0).asWritableLongChunk(), f);
    }

    public interface Builder extends ValueOptions.Builder<TimestampOptions, Builder> {
        Builder format(Format format);
    }
}
