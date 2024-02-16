/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.time.DateTimeUtils;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import java.time.Instant;
import java.util.Optional;

/**
 * Processes a JSON number as an {@link Instant}.
 */
@Immutable
@BuildableStyle
public abstract class InstantNumberOptions extends ValueOptions {

    public enum Format {
        EPOCH_SECONDS, EPOCH_MILLIS, EPOCH_MICROS, EPOCH_NANOS;

        public InstantNumberOptions standard() {
            return builder().format(this).build();
        }

        public InstantNumberOptions strict() {
            return builder()
                    .format(this)
                    .allowNull(false)
                    .allowMissing(false)
                    .build();
        }

        public InstantNumberOptions lenient() {
            return builder()
                    .format(this)
                    .allowNumberFloat(true)
                    .allowString(StringFormat.FLOAT)
                    .build();
        }
    }

    public enum StringFormat {
        NONE, INT, FLOAT
    }

    public static Builder builder() {
        return ImmutableInstantNumberOptions.builder();
    }

    /**
     * The format to use.
     *
     * @return the format
     */
    public abstract Format format();

    /**
     * By default, is {@code true}.
     *
     * @return
     */
    @Default
    public boolean allowNumberInt() {
        return true;
    }

    /**
     * By default, is {@code false}.
     *
     * @return
     */
    @Default
    public boolean allowNumberFloat() {
        return false;
    }

    /**
     * By default, is {@code false}.
     *
     * @return
     */
    @Default
    public StringFormat allowString() {
        return StringFormat.NONE;
    }

    public abstract Optional<Instant> onNull();

    public abstract Optional<Instant> onMissing();

    @Derived
    public long onNullOrDefault() {
        return DateTimeUtils.epochNanos(onNull().orElse(null));
    }

    @Derived
    public long onMissingOrDefault() {
        return DateTimeUtils.epochNanos(onMissing().orElse(null));
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<InstantNumberOptions, Builder> {
        Builder format(Format format);

        Builder onNull(Instant onNull);

        Builder onMissing(Instant onMissing);

        Builder allowNumberInt(boolean allowNumberInt);

        Builder allowNumberFloat(boolean allowNumberFloat);

        Builder allowString(StringFormat allowString);
    }

    @Check
    final void checkNumberFloatInt() {
        if (allowNumberFloat() && !allowNumberInt()) {
            throw new IllegalArgumentException();
        }
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
