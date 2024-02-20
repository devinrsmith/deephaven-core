/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.EnumSet;
import java.util.OptionalDouble;

/**
 * Processes a JSON value as a {@code double}.
 */
@Immutable
@BuildableStyle
public abstract class DoubleOptions extends ValueOptions {

    private static final DoubleOptions STANDARD = builder().build();
    private static final DoubleOptions STRICT = builder().build();
    private static final DoubleOptions LENIENT = builder().build();

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

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<DoubleOptions, Builder> {
        Builder allowNumber(boolean allowNumber);

        Builder allowString(boolean allowString);

        Builder onNull(double onNull);

        Builder onMissing(double onMissing);
    }

    // todo: check float/number must be the same

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
