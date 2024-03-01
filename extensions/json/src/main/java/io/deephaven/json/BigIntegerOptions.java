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
public abstract class BigIntegerOptions extends BoxedOptions<BigInteger> {
    public static Builder builder() {
        return ImmutableBigIntegerOptions.builder();
    }

    public static BigIntegerOptions lenient(boolean allowDecimal) {
        // todo: float
        return builder()
                .allowDecimal(allowDecimal)
                .desiredTypes(allowDecimal ? JsonValueTypes.NUMBER_LIKE : JsonValueTypes.NUMBER_INT_LIKE)
                .build();
    }

    public static BigIntegerOptions standard(boolean allowDecimal) {
        return builder()
                .allowDecimal(allowDecimal)
                .build();
    }

    public static BigIntegerOptions strict(boolean allowDecimal) {
        return builder()
                .allowDecimal(allowDecimal)
                .allowMissing(false)
                .desiredTypes(allowDecimal ? JsonValueTypes.NUMBER : JsonValueTypes.NUMBER_INT.asSet())
                .build();
    }

    @Default
    public boolean allowDecimal() {
        return false;
    }

    @Default
    @Override
    public Set<JsonValueTypes> desiredTypes() {
        return allowDecimal() ? JsonValueTypes.NUMBER_OR_NULL : JsonValueTypes.NUMBER_INT_OR_NULL;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends BoxedOptions.Builder<BigInteger, BigIntegerOptions, Builder> {

        Builder allowDecimal(boolean allowDecimal);
    }

    @Check
    final void checkAllowDecimal() {
        if (allowDecimal() && !allowNumberFloat() && !allowString()) {
            throw new IllegalArgumentException("allowDecimal only makes sense if NUMBER_FLOAT or STRING is enabled");
        }
    }

    @Override
    final EnumSet<JsonValueTypes> allowableTypes() {
        return allowDecimal() ? JsonValueTypes.NUMBER_LIKE : JsonValueTypes.NUMBER_INT_LIKE;
    }
}
