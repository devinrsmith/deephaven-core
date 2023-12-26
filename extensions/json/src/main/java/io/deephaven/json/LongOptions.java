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
import java.util.OptionalLong;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class LongOptions extends ValueOptions {

    private static final LongOptions STANDARD = builder().build();
    private static final LongOptions STRICT = builder()
            .allowNull(false)
            .allowMissing(false)
            .build();
    private static final LongOptions LENIENT = builder()
            .allowNumberFloat(true)
            .allowString(StringFormat.FLOAT)
            .build();

    public static Builder builder() {
        return ImmutableLongOptions.builder();
    }

    /**
     * The standard Long options, equivalent to {@code builder().build()}.
     *
     * @return the standard Long options
     */
    public static LongOptions standard() {
        return STANDARD;
    }

    /**
     * The strict Long options, equivalent to ....
     *
     * @return the strict Long options
     */
    public static LongOptions strict() {
        return STRICT;
    }

    /**
     * The lenient Long options, equivalent to ....
     *
     * @return the lenient Long options
     */
    public static LongOptions lenient() {
        return LENIENT;
    }

    public enum StringFormat {
        NONE, INT, FLOAT
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
    public abstract OptionalLong onNull();

    /**
     * The on-missing value.
     *
     * @return the on-missing value
     */
    public abstract OptionalLong onMissing();

    public interface Builder extends ValueOptions.Builder<LongOptions, Builder> {

        Builder allowNumberInt(boolean allowNumberInt);

        Builder allowNumberFloat(boolean allowNumberFloat);

        Builder allowString(StringFormat allowString);

        Builder onNull(long onNull);

        Builder onMissing(long onMissing);
    }

    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.of(Type.longType());
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new LongValueProcessor(out.get(0).asWritableLongChunk(), new Impl());
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

    private long parseNumberInt(JsonParser parser) throws IOException {
        if (!allowNumberInt()) {
            throw Helpers.mismatch(parser, long.class);
        }
        return parser.getLongValue();
    }

    private long parseNumberFloat(JsonParser parser) throws IOException {
        if (!allowNumberFloat()) {
            throw Helpers.mismatch(parser, long.class);
        }
        // May lose info
        return parser.getLongValue();
    }

    private long parseString(JsonParser parser) throws IOException {
        switch (allowString()) {
            case NONE:
                throw Helpers.mismatch(parser, long.class);
            case INT:
                return Helpers.parseStringAsLong(parser);
            case FLOAT:
                // Need to parse as BigDecimal to have 64-bit long range
                return Helpers.parseStringAsBigDecimal(parser).longValue();
        }
        throw new IllegalStateException();
    }

    private long parseNull(JsonParser parser) throws IOException {
        if (!allowNull()) {
            throw Helpers.mismatch(parser, long.class);
        }
        return onNull().orElse(QueryConstants.NULL_LONG);
    }

    private long parseMissing(JsonParser parser) throws IOException {
        if (!allowMissing()) {
            throw Helpers.mismatchMissing(parser, long.class);
        }
        return onMissing().orElse(QueryConstants.NULL_LONG);
    }

    private class Impl implements ToLong {
        @Override
        public long parseValue(JsonParser parser) throws IOException {
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
            throw Helpers.mismatch(parser, long.class);
        }

        @Override
        public long parseMissing(JsonParser parser) throws IOException {
            return LongOptions.this.parseMissing(parser);
        }
    }
}
