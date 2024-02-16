/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

/**
 * Processes a JSON value by skipping it.
 */
@Immutable
@BuildableStyle
public abstract class SkipOptions extends ValueOptions {

    public static Builder builder() {
        return ImmutableSkipOptions.builder();
    }

    public static SkipOptions numberInt() {
        return builder().allowNumberInt(true).build();
    }

    public static SkipOptions numberFloat() {
        return builder().allowNumberFloat(true).build();
    }

    public static SkipOptions string() {
        return builder().allowString(true).build();
    }

    public static SkipOptions _boolean() {
        return builder().allowBoolean(true).build();
    }

    public static SkipOptions object() {
        return builder().allowObject(true).build();
    }

    public static SkipOptions array() {
        return builder().allowArray(true).build();
    }

    public static SkipOptions lenient() {
        return builder()
                .allowNumberInt(true)
                .allowNumberFloat(true)
                .allowString(true)
                .allowBoolean(true)
                .allowObject(true)
                .allowArray(true)
                .allowNull(true)
                .allowMissing(true)
                .build();
    }

    public abstract boolean allowNumberInt();

    public abstract boolean allowNumberFloat();

    public abstract boolean allowString();

    public abstract boolean allowBoolean();

    public abstract boolean allowObject();

    public abstract boolean allowArray();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<SkipOptions, Builder> {
        Builder allowNumberInt(boolean allowNumberInt);

        Builder allowNumberFloat(boolean allowNumberInt);

        Builder allowString(boolean allowString);

        Builder allowBoolean(boolean allowBoolean);

        Builder allowObject(boolean allowObject);

        Builder allowArray(boolean allowArray);
    }
}
