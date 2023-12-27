/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.io.IOException;
import java.util.List;
import java.util.OptionalDouble;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class DoubleOptions extends ValueOptions {

    private static final DoubleOptions STANDARD = builder().build();
    private static final DoubleOptions STRICT = builder()
            .allowNull(false)
            .allowMissing(false)
            .build();
    private static final DoubleOptions LENIENT = builder()
            .allowString(true)
            .build();

    public static Builder builder() {
        return ImmutableDoubleOptions.builder();
    }

    /**
     * The standard double options, equivalent to {@code builder().build()}.
     *
     * @return the standard double options
     */
    public static DoubleOptions standard() {
        return STANDARD;
    }

    /**
     * The strict double options, equivalent to
     * {@code builder().onValue(ToDoubleImpl.strict()).allowMissing(false).build()}.
     *
     * @return the strict double options
     */
    public static DoubleOptions strict() {
        return STRICT;
    }

    /**
     * The lenient double options, equivalent to {@code builder().onValue(ToDoubleImpl.lenient()).build()}.
     *
     * @return the lenient double options
     */
    public static DoubleOptions lenient() {
        return LENIENT;
    }

    public abstract OptionalDouble onNull();

    /**
     * The onMissing value to use. Must not set if {@link #allowMissing()} is {@code false}.
     **/
    public abstract OptionalDouble onMissing();

    @Default
    public boolean allowNumber() {
        return true;
    }

    @Default
    public boolean allowString() {
        return false;
    }

    public final SkipOptions skip() {
        return SkipOptions.builder()
                .allowNumberFloat(allowNumber())
                .allowNumberInt(allowNumber())
                .allowString(allowString())
                .allowNull(allowNull())
                .allowMissing(allowMissing())
                .build();
    }

    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.of(Type.doubleType());
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new DoubleValueProcessor(out.get(0).asWritableDoubleChunk(), new Impl());
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

    private double onNullOrDefault() {
        return onNull().orElse(QueryConstants.NULL_DOUBLE);
    }

    private double onMissingOrDefault() {
        return onMissing().orElse(QueryConstants.NULL_DOUBLE);
    }

    private double parseNumberIntOrFloat(JsonParser parser) throws IOException {
        if (!allowNumber()) {
            throw Helpers.mismatch(parser, double.class);
        }
        return parser.getDoubleValue();
    }

    private double parseString(JsonParser parser) throws IOException {
        if (!allowString()) {
            throw Helpers.mismatch(parser, double.class);
        }
        return Helpers.parseStringAsDouble(parser);
    }

    private double parseNull(JsonParser parser) throws MismatchedInputException {
        if (!allowNull()) {
            throw Helpers.mismatch(parser, double.class);
        }
        return onNullOrDefault();
    }

    private double parseMissing(JsonParser parser) throws MismatchedInputException {
        if (!allowMissing()) {
            throw Helpers.mismatchMissing(parser, double.class);
        }
        return onMissingOrDefault();
    }

    class Impl implements DoubleValueProcessor.ToDouble {
        @Override
        public double parseValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case VALUE_NUMBER_INT:
                case VALUE_NUMBER_FLOAT:
                    return parseNumberIntOrFloat(parser);
                case VALUE_STRING:
                    return parseString(parser);
                case VALUE_NULL:
                    return parseNull(parser);
            }
            throw Helpers.mismatch(parser, double.class);
        }

        @Override
        public double parseMissing(JsonParser parser) throws IOException {
            return DoubleOptions.this.parseMissing(parser);
        }
    }

    public interface Builder extends ValueOptions.Builder<DoubleOptions, Builder> {
        Builder allowNumber(boolean allowNumber);

        Builder allowString(boolean allowString);

        Builder onNull(double onNull);

        Builder onMissing(double onMissing);
    }
}
