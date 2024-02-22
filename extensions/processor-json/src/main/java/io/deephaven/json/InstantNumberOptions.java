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
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

/**
 * Processes a JSON number as an {@link Instant}.
 */
@Immutable
@BuildableStyle
public abstract class InstantNumberOptions extends ValueOptions {

    public enum Format {
        EPOCH_SECONDS, EPOCH_MILLIS, EPOCH_MICROS, EPOCH_NANOS;


        public InstantNumberOptions lenient() {
            return builder()
                    .desiredTypes(JsonValueTypes.NUMBER_LIKE)
                    .stringFormat(StringNumberFormat.FLOAT)
                    .build();
        }

        public InstantNumberOptions standard() {
            return builder().build();
        }

        public InstantNumberOptions strict() {
            return builder()
                    .allowMissing(false)
                    .desiredTypes(JsonValueTypes.NUMBER_INT.asSet())
                    .build();
        }
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

    public abstract Optional<Instant> onNull();

    public abstract Optional<Instant> onMissing();

    public abstract Optional<StringNumberFormat> stringFormat();

    @Default
    @Override
    public Set<JsonValueTypes> desiredTypes() {
        return JsonValueTypes.NUMBER_INT_OR_NULL;
    }

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

        Builder stringFormat(StringNumberFormat allowString);
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
    final void stringFormatCheck() {
        if (stringFormat().isPresent() && !allowString()) {
            throw new IllegalArgumentException("stringFormat is only applicable when strings are allowed");
        }
    }

    @Override
    final EnumSet<JsonValueTypes> allowableTypes() {
        return JsonValueTypes.NUMBER_LIKE;
    }
}
