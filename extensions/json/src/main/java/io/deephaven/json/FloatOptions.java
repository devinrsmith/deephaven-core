/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class FloatOptions extends ValueOptions {

    private static final FloatOptions STANDARD = builder().build();
    private static final FloatOptions STRICT = builder()
            .allowNull(false)
            .allowMissing(false)
            .build();
    private static final FloatOptions LENIENT = builder()
            .allowString(true)
            .build();

    public static Builder builder() {
        return ImmutableFloatOptions.builder();
    }

    /**
     * The standard float options, equivalent to {@code builder().build()}.
     *
     * @return the standard float options
     */
    public static FloatOptions standard() {
        return STANDARD;
    }

    /**
     * The strict float options, equivalent to
     * {@code builder().onValue(ToFloatImpl.strict()).allowMissing(false).build()}.
     *
     * @return the strict float options
     */
    public static FloatOptions strict() {
        return STRICT;
    }

    /**
     * The lenient float options, equivalent to {@code builder().onValue(ToFloatImpl.lenient()).build()}.
     *
     * @return the lenient float options
     */
    public static FloatOptions lenient() {
        return LENIENT;
    }

    @Nullable
    public abstract Float onNull();

    /**
     * The onMissing value to use. Must not set if {@link #allowMissing()} is {@code false}.
     **/
    @Nullable
    public abstract Float onMissing();

    @Default
    public boolean allowString() {
        return false;
    }

    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.of(Type.floatType());
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new FloatValueProcessor(out.get(0).asWritableFloatChunk(), new Impl());
    }

    @Check
    final void checkOnNull() {
        if (!allowNull() && onNull() != null) {
            throw new IllegalArgumentException();
        }
    }

    @Check
    final void checkOnMissing() {
        if (!allowMissing() && onMissing() != null) {
            throw new IllegalArgumentException();
        }
    }

    private float onNullOrDefault() {
        final Float onNull = onNull();
        return onNull != null ? onNull : QueryConstants.NULL_FLOAT;
    }

    private float onMissingOrDefault() {
        final Float onMissing = onMissing();
        return onMissing != null ? onMissing : QueryConstants.NULL_FLOAT;
    }

    class Impl implements ToFloat {
        @Override
        public float parseValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case VALUE_NULL:
                    if (!allowNull()) {
                        throw Helpers.mismatch(parser, float.class);
                    }
                    return onNullOrDefault();
                case VALUE_NUMBER_FLOAT:
                case VALUE_NUMBER_INT:
                    return parser.getFloatValue();
                case VALUE_STRING:
                    if (!allowString()) {
                        throw Helpers.mismatch(parser, float.class);
                    }
                    return Helpers.parseStringAsFloat(parser);
            }
            throw Helpers.mismatch(parser, float.class);
        }

        @Override
        public float parseMissing(JsonParser parser) throws IOException {
            if (!allowMissing()) {
                throw Helpers.mismatchMissing(parser, float.class);
            }
            return onMissingOrDefault();
        }
    }

    public interface Builder extends ValueOptions.Builder<FloatOptions, Builder> {
        Builder allowString(boolean allowString);

        Builder onNull(Float onNull);

        Builder onMissing(Float onMissing);
    }
}
