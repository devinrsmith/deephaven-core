/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.math.BigInteger;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

/**
 * Processes a JSON value as a {@link BigInteger}.
 */
@Immutable
@BuildableStyle
public abstract class BigIntegerOptions extends ValueOptions {
    public static Builder builder() {
        return ImmutableBigIntegerOptions.builder();
    }

    public static BigIntegerOptions lenient() {
        // todo: float
        return builder().desiredTypes(JsonValueTypes.NUMBER_LIKE).build();
    }

    public static BigIntegerOptions standard() {
        return builder().build();
    }

    public static BigIntegerOptions strict() {
        return builder()
                .allowMissing(false)
                .desiredTypes(JsonValueTypes.NUMBER_INT.asSet())
                .build();
    }

    public abstract Optional<StringNumberFormat> stringFormat();

    public abstract Optional<BigInteger> onNull();

    public abstract Optional<BigInteger> onMissing();

    @Default
    @Override
    public Set<JsonValueTypes> desiredTypes() {
        return JsonValueTypes.NUMBER_INT_OR_NULL;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<BigIntegerOptions, Builder> {

        Builder onNull(BigInteger onNull);

        Builder onMissing(BigInteger onMissing);
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

    @Check
    final void stringFormatCheck() {
        if (stringFormat().isPresent() && !allowString()) {
            throw new IllegalArgumentException("stringFormat is only applicable when strings are allowed");
        }
    }

    @Override
    final EnumSet<JsonValueTypes> allowableTypes() {
        return JsonValueTypes.NUMBER_LIKE;
    }
}
