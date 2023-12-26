/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class BigDecimalOptions extends ValueOptions {
    private static final BigDecimalOptions STANDARD = builder().build();
    private static final BigDecimalOptions STRICT = builder().allowNull(false).allowMissing(false).build();
    private static final BigDecimalOptions LENIENT = builder().allowString(true).build();

    public static Builder builder() {
        return ImmutableBigDecimalOptions.builder();
    }

    public static BigDecimalOptions standard() {
        return STANDARD;
    }

    public static BigDecimalOptions strict() {
        return STRICT;
    }

    public static BigDecimalOptions lenient() {
        return LENIENT;
    }

    @Default
    public boolean allowNumber() {
        return true;
    }

    @Default
    public boolean allowString() {
        return false;
    }

    public abstract Optional<BigDecimal> onNull();

    public abstract Optional<BigDecimal> onMissing();

    public interface Builder extends ValueOptions.Builder<BigDecimalOptions, Builder> {
        Builder allowNumber(boolean allowNumber);

        Builder allowString(boolean allowString);

        Builder onNull(BigDecimal onNull);

        Builder onMissing(BigDecimal onMissing);
    }

    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.of(Type.ofCustom(BigDecimal.class));
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new ObjectValueProcessor<>(out.get(0).asWritableObjectChunk(), new Impl());
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

    private BigDecimal parseNumberIntOrFloat(JsonParser parser) throws IOException {
        if (!allowNumber()) {
            throw Helpers.mismatch(parser, BigDecimal.class);
        }
        return parser.getDecimalValue();
    }

    private BigDecimal parseString(JsonParser parser) throws IOException {
        if (!allowString()) {
            throw Helpers.mismatch(parser, BigDecimal.class);
        }
        return Helpers.parseStringAsBigDecimal(parser);
    }

    private BigDecimal parseNull(JsonParser parser) throws MismatchedInputException {
        if (!allowNull()) {
            throw Helpers.mismatch(parser, BigDecimal.class);
        }
        return onNull().orElse(null);
    }

    private BigDecimal parseMissing(JsonParser parser) throws MismatchedInputException {
        if (!allowMissing()) {
            throw Helpers.mismatchMissing(parser, BigDecimal.class);
        }
        return onMissing().orElse(null);
    }

    private class Impl implements ToObject<BigDecimal> {
        @Override
        public BigDecimal parseValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case VALUE_NUMBER_INT:
                case VALUE_NUMBER_FLOAT:
                    return parseNumberIntOrFloat(parser);
                case VALUE_STRING:
                    return parseString(parser);
                case VALUE_NULL:
                    return parseNull(parser);
            }
            throw Helpers.mismatch(parser, BigDecimal.class);
        }

        @Override
        public BigDecimal parseMissing(JsonParser parser) throws IOException {
            return BigDecimalOptions.this.parseMissing(parser);
        }
    }
}
