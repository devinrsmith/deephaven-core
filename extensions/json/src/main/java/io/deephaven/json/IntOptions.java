/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.EnumSet;
import java.util.OptionalInt;
import java.util.Set;

/**
 * Processes a JSON value as an {@code int}.
 */
@Immutable
@BuildableStyle
public abstract class IntOptions extends ValueOptions {

    public static Builder builder() {
        return ImmutableIntOptions.builder();
    }

    /**
     * The lenient Int options, equivalent to ....
     *
     * @return the lenient Int options
     */
    public static IntOptions lenient() {
        return builder()
                .desiredTypes(JsonValueTypes.NUMBER_INT_LIKE)
                .build();
    }

    /**
     * The standard Int options, equivalent to {@code builder().build()}.
     *
     * @return the standard Int options
     */
    public static IntOptions standard() {
        return builder().build();
    }

    /**
     * The strict Int options, equivalent to ....
     *
     * @return the strict Int options
     */
    public static IntOptions strict() {
        return builder()
                .allowMissing(false)
                .desiredTypes(JsonValueTypes.NUMBER_INT.asSet())
                .build();
    }

    @Default
    public boolean allowDecimal() {
        return false;
    }

    /**
     * The desired types. By default, is TODO update based on allowDecimal {@link JsonValueTypes#NUMBER_INT} and
     * {@link JsonValueTypes#NULL}.
     */
    @Default
    @Override
    public Set<JsonValueTypes> desiredTypes() {
        return allowDecimal() ? JsonValueTypes.NUMBER_OR_NULL : JsonValueTypes.NUMBER_INT_OR_NULL;
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


    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<IntOptions, Builder> {

        Builder allowDecimal(boolean allowDecimal);

        Builder onNull(int onNull);

        Builder onMissing(int onMissing);
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
