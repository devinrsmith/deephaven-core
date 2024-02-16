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

    private static final IntOptions LENIENT = builder()
            .addAllDesiredTypes(JsonValueTypes.NUMBER_LIKE)
            .build();
    private static final IntOptions STANDARD = builder()
            .addDesiredTypes(JsonValueTypes.NUMBER_INT, JsonValueTypes.NULL)
            .build();
    private static final IntOptions STRICT = builder()
            .allowMissing(false)
            .addDesiredTypes(JsonValueTypes.NUMBER_INT)
            .build();

    public static Builder builder() {
        return ImmutableIntOptions.builder();
    }

    public enum StringFormat {
        NONE, INT, FLOAT
    }

    /**
     * The lenient Int options, equivalent to ....
     *
     * @return the lenient Int options
     */
    public static IntOptions lenient() {
        return LENIENT;
    }

    /**
     * The standard Int options, equivalent to {@code builder().build()}.
     *
     * @return the standard Int options
     */
    public static IntOptions standard() {
        return STANDARD;
    }

    /**
     * The strict Int options, equivalent to ....
     *
     * @return the strict Int options
     */
    public static IntOptions strict() {
        return STRICT;
    }

    /**
     * The desired types. By default, is {@link JsonValueTypes#NUMBER_INT} and {@link JsonValueTypes#NULL}.
     */
    @Override
    public Set<JsonValueTypes> desiredTypes() {
        return JsonValueTypes.NUMBER_INT_OR_NULL;
    }

    /**
     * If parsing JSON strings is supported. By default, is {@code false}.
     *
     * @return allow string
     */
    @Default
    public StringFormat allowString2() {
        return StringFormat.NONE;
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

        Builder allowString(StringFormat allowString);

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

    @Override
    final EnumSet<JsonValueTypes> allowableTypes() {
        return JsonValueTypes.NUMBER_LIKE;
    }
}
