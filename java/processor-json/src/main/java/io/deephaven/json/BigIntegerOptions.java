/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class BigIntegerOptions extends ValueOptions {
    private static final BigIntegerOptions STANDARD = builder().build();
    private static final BigIntegerOptions STRICT = builder().allowNull(false).allowMissing(false).build();
    private static final BigIntegerOptions LENIENT =
            builder().allowNumberFloat(true).allowString(StringFormat.FLOAT).build();

    public static Builder builder() {
        return ImmutableBigIntegerOptions.builder();
    }

    public static BigIntegerOptions standard() {
        return STANDARD;
    }

    public static BigIntegerOptions strict() {
        return STRICT;
    }

    public static BigIntegerOptions lenient() {
        return LENIENT;
    }

    public enum StringFormat {
        NONE, INT, FLOAT
    }

    @Default
    public boolean allowNumberInt() {
        return true;
    }

    @Default
    public boolean allowNumberFloat() {
        return false;
    }

    @Default
    public StringFormat allowString() {
        return StringFormat.NONE;
    }

    public abstract Optional<BigInteger> onNull();

    public abstract Optional<BigInteger> onMissing();

    public interface Builder extends ValueOptions.Builder<BigIntegerOptions, Builder> {
        Builder allowNumberInt(boolean allowNumberInt);

        Builder allowNumberFloat(boolean allowNumberFloat);

        Builder allowString(StringFormat allowString);

        Builder onNull(BigInteger onNull);

        Builder onMissing(BigInteger onMissing);
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
        return Stream.of(Type.ofCustom(BigInteger.class));
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new ObjectValueProcessor<>(out.get(0).asWritableObjectChunk(), new Impl());
    }

    @Check
    final void checkNumberFloatInt() {
        if (allowNumberFloat() && !allowNumberInt()) {
            throw new IllegalArgumentException();
        }
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

    private BigInteger parseNumberInt(JsonParser parser) throws IOException {
        if (!allowNumberInt()) {
            throw Helpers.mismatch(parser, BigInteger.class);
        }
        return parser.getBigIntegerValue();
    }

    private BigInteger parseNumberFloat(JsonParser parser) throws IOException {
        if (!allowNumberFloat()) {
            throw Helpers.mismatch(parser, BigInteger.class);
        }
        return parser.getBigIntegerValue();
    }

    private BigInteger parseString(JsonParser parser) throws IOException {
        switch (allowString()) {
            case NONE:
                throw Helpers.mismatch(parser, BigInteger.class);
            case INT:
                return Helpers.parseStringAsBigInteger(parser);
            case FLOAT:
                return Helpers.parseStringAsBigDecimal(parser).toBigInteger();
        }
        throw new IllegalStateException();
    }

    private BigInteger parseNull(JsonParser parser) throws IOException {
        if (!allowNull()) {
            throw Helpers.mismatch(parser, BigInteger.class);
        }
        return onNull().orElse(null);
    }

    private BigInteger parseMissing(JsonParser parser) throws IOException {
        if (!allowMissing()) {
            throw Helpers.mismatchMissing(parser, BigInteger.class);
        }
        return onMissing().orElse(null);
    }

    private class Impl implements ObjectValueProcessor.ToObject<BigInteger> {
        @Override
        public BigInteger parseValue(JsonParser parser) throws IOException {
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
            throw Helpers.mismatch(parser, BigInteger.class);
        }

        @Override
        public BigInteger parseMissing(JsonParser parser) throws IOException {
            return BigIntegerOptions.this.parseMissing(parser);
        }
    }
}
