/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import javax.annotation.Nullable;
import java.util.EnumSet;

/**
 * Processes a JSON value as a {@code float}.
 */
@Immutable
@BuildableStyle
public abstract class FloatOptions extends ValueOptions {

    private static final FloatOptions STANDARD = builder().build();
    private static final FloatOptions STRICT = builder().build();
    private static final FloatOptions LENIENT = builder().build();

    public static Builder builder() {
        return ImmutableFloatOptions.builder();
    }

    /**
     * The standard float options, equivalent to {@code builder().build()}.
     *
     * @return the standard float options
     */
    public static FloatOptions standard() {
        return STANDARD;
    }

    /**
     * The strict float options, equivalent to
     * {@code builder().onValue(ToFloatImpl.strict()).allowMissing(false).build()}.
     *
     * @return the strict float options
     */
    public static FloatOptions strict() {
        return STRICT;
    }

    /**
     * The lenient float options, equivalent to {@code builder().onValue(ToFloatImpl.lenient()).build()}.
     *
     * @return the lenient float options
     */
    public static FloatOptions lenient() {
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

    @Nullable
    public abstract Float onNull();

    /**
     * The onMissing value to use. Must not set if {@link #allowMissing()} is {@code false}.
     **/
    @Nullable
    public abstract Float onMissing();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<FloatOptions, Builder> {
        Builder allowNumber(boolean allowNumber);

        Builder allowString(boolean allowString);

        Builder onNull(Float onNull);

        Builder onMissing(Float onMissing);
    }

    @Check
    final void checkOnNull() {
        if (!allowNull() && onNull() != null) {
            throw new IllegalArgumentException();
        }
    }

    @Check
    final void checkOnMissing() {
        if (!allowMissing() && onMissing() != null) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    final EnumSet<JsonValueTypes> allowableTypes() {
        return JsonValueTypes.NUMBER_LIKE;
    }
}
