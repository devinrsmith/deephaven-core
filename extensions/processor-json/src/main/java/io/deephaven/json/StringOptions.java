/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Optional;

/**
 * Processes a JSON value as a {@link String}.
 */
@Immutable
@BuildableStyle
public abstract class StringOptions extends ValueOptions {
    private static final StringOptions STANDARD = builder().build();
    private static final StringOptions STRICT = builder()
            .allowNull(false)
            .allowMissing(false)
            .build();
    private static final StringOptions LENIENT = builder()
            .allowNumberInt(true)
            .allowNumberFloat(true)
            .allowBoolean(true)
            .build();

    public static Builder builder() {
        return ImmutableStringOptions.builder();
    }


    public static StringOptions standard() {
        return STANDARD;
    }

    public static StringOptions strict() {
        return STRICT;
    }

    public static StringOptions lenient() {
        return LENIENT;
    }

    /**
     * If a
     * 
     * @return
     */
    @Default
    public boolean allowString() {
        return true;
    }

    @Default
    public boolean allowNumberInt() {
        return false;
    }

    @Default
    public boolean allowNumberFloat() {
        return false;
    }

    @Default
    public boolean allowBoolean() {
        return false;
    }

    public abstract Optional<String> onNull();

    public abstract Optional<String> onMissing();

    public final SkipOptions skip() {
        return SkipOptions.builder()
                .allowString(allowString())
                .allowNumberInt(allowNumberInt())
                .allowNumberFloat(allowNumberFloat())
                .allowBoolean(allowBoolean())
                .allowNull(allowNull())
                .allowMissing(allowMissing())
                .build();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<StringOptions, Builder> {
        Builder allowString(boolean allowString);

        Builder allowNumberInt(boolean allowNumberInt);

        Builder allowNumberFloat(boolean allowNumberFloat);

        Builder allowBoolean(boolean allowBoolean);

        Builder onNull(String onNull);

        Builder onMissing(String onMissing);
    }

    @Override
    StringOptions withMissingSupport() {
        if (allowMissing()) {
            return this;
        }
        final Builder builder = builder()
                .allowString(allowString())
                .allowNumberInt(allowNumberInt())
                .allowNumberFloat(allowNumberFloat())
                .allowNull(allowNull())
                .allowMissing(true);
        onNull().ifPresent(builder::onNull);
        // todo: option for onMissing?
        return builder.build();
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
