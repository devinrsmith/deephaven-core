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

import java.util.OptionalLong;

@Immutable
@BuildableStyle
public abstract class LongOptions extends ValueOptions {

    private static final LongOptions STANDARD = builder().build();
    private static final LongOptions STRICT = builder()
            .allowNull(false)
            .allowMissing(false)
            .build();
    private static final LongOptions LENIENT = builder()
            .allowNumberFloat(true)
            .allowString(StringFormat.FLOAT)
            .build();

    public static Builder builder() {
        return ImmutableLongOptions.builder();
    }

    /**
     * The standard Long options, equivalent to {@code builder().build()}.
     *
     * @return the standard Long options
     */
    public static LongOptions standard() {
        return STANDARD;
    }

    /**
     * The strict Long options, equivalent to ....
     *
     * @return the strict Long options
     */
    public static LongOptions strict() {
        return STRICT;
    }

    /**
     * The lenient Long options, equivalent to ....
     *
     * @return the lenient Long options
     */
    public static LongOptions lenient() {
        return LENIENT;
    }

    public enum StringFormat {
        NONE, INT, FLOAT
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
    public abstract OptionalLong onNull();

    /**
     * The on-missing value.
     *
     * @return the on-missing value
     */
    public abstract OptionalLong onMissing();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<LongOptions, Builder> {

        Builder allowNumberInt(boolean allowNumberInt);

        Builder allowNumberFloat(boolean allowNumberFloat);

        Builder allowString(StringFormat allowString);

        Builder onNull(long onNull);

        Builder onMissing(long onMissing);
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
