/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.time.Instant;
import java.util.EnumSet;
import java.util.Set;

/**
 * Processes a JSON number (or string that is a number) as an {@link Instant}.
 */
@Immutable
@BuildableStyle
public abstract class InstantNumberOptions extends BoxedOptions<Instant> {

    public enum Format {
        EPOCH_SECONDS, EPOCH_MILLIS, EPOCH_MICROS, EPOCH_NANOS;

        public InstantNumberOptions lenient(boolean allowDecimal) {
            return builder()
                    .format(this)
                    .allowDecimal(allowDecimal)
                    .desiredTypes(allowDecimal ? JsonValueTypes.NUMBER_LIKE : JsonValueTypes.INT_LIKE)
                    .build();
        }

        public InstantNumberOptions standard(boolean allowDecimal) {
            return builder()
                    .format(this)
                    .allowDecimal(allowDecimal)
                    .build();
        }

        public InstantNumberOptions strict(boolean allowDecimal) {
            return builder()
                    .format(this)
                    .allowDecimal(allowDecimal)
                    .allowMissing(false)
                    .desiredTypes(allowDecimal ? JsonValueTypes.NUMBER : JsonValueTypes.INT.asSet())
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

    @Default
    public boolean allowDecimal() {
        return false;
    }

    @Default
    @Override
    public Set<JsonValueTypes> desiredTypes() {
        return allowDecimal() ? JsonValueTypes.NUMBER_OR_NULL : JsonValueTypes.INT_OR_NULL;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends BoxedOptions.Builder<Instant, InstantNumberOptions, Builder> {
        Builder format(Format format);

        Builder allowDecimal(boolean allowDecimal);
    }

    @Check
    final void checkAllowDecimal() {
        if (allowDecimal() && !allowNumberFloat() && !allowString()) {
            throw new IllegalArgumentException("allowDecimal only makes sense if NUMBER_FLOAT or STRING is enabled");
        }
    }

    @Override
    final EnumSet<JsonValueTypes> allowableTypes() {
        return allowDecimal() ? JsonValueTypes.NUMBER_LIKE : JsonValueTypes.INT_LIKE;
    }
}
