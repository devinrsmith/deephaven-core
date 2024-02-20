/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.math.BigInteger;
import java.util.EnumSet;
import java.util.Optional;

/**
 * Processes a JSON value as a {@link BigInteger}.
 */
@Immutable
@BuildableStyle
public abstract class BigIntegerOptions extends ValueOptions {
    private static final BigIntegerOptions STANDARD = builder().build();
    private static final BigIntegerOptions STRICT = builder().build();
    private static final BigIntegerOptions LENIENT = builder().build();

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

    public abstract Optional<BigInteger> onNull();

    public abstract Optional<BigInteger> onMissing();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<BigIntegerOptions, Builder> {

        Builder onNull(BigInteger onNull);

        Builder onMissing(BigInteger onMissing);
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

    @Override
    final EnumSet<JsonValueTypes> allowableTypes() {
        return JsonValueTypes.NUMBER_LIKE;
    }
}
