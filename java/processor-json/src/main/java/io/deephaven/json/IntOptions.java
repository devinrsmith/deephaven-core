/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.OptionalInt;

@Immutable
@BuildableStyle
public abstract class IntOptions extends ValueOptions {

    private static final IntOptions STANDARD = builder().build();
    private static final IntOptions STRICT = builder()
            .allowNull(false)
            .allowMissing(false)
            .build();
    private static final IntOptions LENIENT = builder()
            .allowNumberFloat(true)
            .allowString(StringFormat.FLOAT)
            .build();

    public static Builder builder() {
        return ImmutableIntOptions.builder();
    }

    public enum StringFormat {
        NONE, INT, FLOAT
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
     * The lenient Int options, equivalent to ....
     *
     * @return the lenient Int options
     */
    public static IntOptions lenient() {
        return LENIENT;
    }

    /**
     * If parsing {@link JsonToken#VALUE_NUMBER_INT} is supported. By default, is {@code true}.
     *
     * @return allow number int
     * @see #parseNumberInt(JsonParser)
     */
    @Default
    public boolean allowNumberInt() {
        return true;
    }

    /**
     * If parsing {@link JsonToken#VALUE_NUMBER_FLOAT} is supported. By default, is {@code false}.
     *
     * @return allow number float
     * @see #parseNumberFloat(JsonParser)
     */
    @Default
    public boolean allowNumberFloat() {
        return false;
    }

    /**
     * If parsing {@link JsonToken#VALUE_STRING} is supported. By default, is {@code false}.
     *
     * @return allow string
     * @see #parseString(JsonParser)
     */
    @Default
    public StringFormat allowString() {
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

    public final SkipOptions skip() {
        return SkipOptions.builder()
                .allowNumberInt(allowNumberInt())
                .allowNumberFloat(allowNumberFloat())
                .allowString(allowString() != StringFormat.NONE)
                .allowNull(allowNull())
                .allowMissing(allowMissing())
                .allowBoolean(false)
                .allowObject(false)
                .allowArray(false)
                .build();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<IntOptions, Builder> {

        Builder allowNumberInt(boolean allowNumberInt);

        Builder allowNumberFloat(boolean allowNumberFloat);

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
}
