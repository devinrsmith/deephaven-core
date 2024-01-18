/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.io.IOException;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class IntOptions extends ValueOptions {

    private static final IntOptions STANDARD = builder().build();
    private static final IntOptions STRICT = builder()
            .allowNull(false)
            .allowMissing(false)
            .build();
    private static final IntOptions LENIENT = builder()
            .allowNumberFloat(true)
            .allowString(StringFormat.FLOAT)
            .build();

    public static Builder builder() {
        return ImmutableIntOptions.builder();
    }

    public enum StringFormat {
        NONE, INT, FLOAT
    }

    /**
     * The standard Int options, equivalent to {@code builder().build()}.
     *
     * @return the standard Int options
     */
    public static IntOptions standard() {
        return STANDARD;
    }

    /**
     * The strict Int options, equivalent to ....
     *
     * @return the strict Int options
     */
    public static IntOptions strict() {
        return STRICT;
    }

    /**
     * The lenient Int options, equivalent to ....
     *
     * @return the lenient Int options
     */
    public static IntOptions lenient() {
        return LENIENT;
    }

    /**
     * If parsing {@link JsonToken#VALUE_NUMBER_INT} is supported. By default, is {@code true}.
     *
     * @return allow number int
     * @see #parseNumberInt(JsonParser)
     */
    @Default
    public boolean allowNumberInt() {
        return true;
    }

    /**
     * If parsing {@link JsonToken#VALUE_NUMBER_FLOAT} is supported. By default, is {@code false}.
     *
     * @return allow number float
     * @see #parseNumberFloat(JsonParser)
     */
    @Default
    public boolean allowNumberFloat() {
        return false;
    }

    /**
     * If parsing {@link JsonToken#VALUE_STRING} is supported. By default, is {@code false}.
     *
     * @return allow string
     * @see #parseString(JsonParser)
     */
    @Default
    public StringFormat allowString() {
        return StringFormat.NONE;
    }

    /**
     * The on-null value.
     *
     * @return the on-null value
     */
    public abstract OptionalInt onNull();

    /**
     * The on-missing value.
     *
     * @return the on-missing value
     */
    public abstract OptionalInt onMissing();

    public final SkipOptions skip() {
        return SkipOptions.builder()
                .allowNumberInt(allowNumberInt())
                .allowNumberFloat(allowNumberFloat())
                .allowString(allowString() != StringFormat.NONE)
                .allowNull(allowNull())
                .allowMissing(allowMissing())
                .allowBoolean(false)
                .allowObject(false)
                .allowArray(false)
                .build();
    }

    public interface Builder extends ValueOptions.Builder<IntOptions, Builder> {

        Builder allowNumberInt(boolean allowNumberInt);

        Builder allowNumberFloat(boolean allowNumberFloat);

        Builder allowString(StringFormat allowString);

        Builder onNull(int onNull);

        Builder onMissing(int onMissing);
    }

    @Override
    final int outputCount() {
        return 1;
    }

    @Override
    final Stream<List<String>> paths() {
        return Stream.of(List.of());
    }

    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.of(Type.intType());
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new IntValueProcessor(out.get(0).asWritableIntChunk(), new Impl());
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

    private int parseNumberInt(JsonParser parser) throws IOException {
        if (!allowNumberInt()) {
            throw Helpers.mismatch(parser, int.class);
        }
        return parser.getIntValue();
    }

    private int parseNumberFloat(JsonParser parser) throws IOException {
        if (!allowNumberFloat()) {
            throw Helpers.mismatch(parser, int.class);
        }
        // May lose info
        return parser.getIntValue();
    }

    private int parseString(JsonParser parser) throws IOException {
        switch (allowString()) {
            case NONE:
                throw Helpers.mismatch(parser, int.class);
            case INT:
                return Helpers.parseStringAsInt(parser);
            case FLOAT:
                // Need to parse as double to have 32-bit int range
                return (int) Helpers.parseStringAsDouble(parser);
        }
        throw new IllegalStateException();
    }

    private int parseNull(JsonParser parser) throws IOException {
        if (!allowNull()) {
            throw Helpers.mismatch(parser, int.class);
        }
        return onNull().orElse(QueryConstants.NULL_INT);
    }

    private int parseMissing(JsonParser parser) throws IOException {
        if (!allowMissing()) {
            throw Helpers.mismatchMissing(parser, int.class);
        }
        return onMissing().orElse(QueryConstants.NULL_INT);
    }

    private class Impl implements IntValueProcessor.ToInt {
        @Override
        public int parseValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case VALUE_NUMBER_INT:
                    return parseNumberInt(parser);
                case VALUE_NUMBER_FLOAT:
                    return parseNumberFloat(parser);
                case VALUE_STRING:
                    return parseString(parser);
                case VALUE_NULL:
                    return parseNull(parser);
            }
            throw Helpers.mismatch(parser, int.class);
        }

        @Override
        public int parseMissing(JsonParser parser) throws IOException {
            return IntOptions.this.parseMissing(parser);
        }
    }
}
