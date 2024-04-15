//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Set;

/**
 * Processes a JSON value as a {@code double}.
 */
@Immutable
@BuildableStyle
public abstract class DoubleOptions extends ValueOptionsSingleValueBase<Double> {


    public static Builder builder() {
        return ImmutableDoubleOptions.builder();
    }

    /**
     * The lenient double options. Allows missing and accepts {@link JsonValueTypes#numberLike()}.
     *
     * @return the lenient double options
     */
    public static DoubleOptions lenient() {
        return builder()
                .allowedTypes(JsonValueTypes.numberLike())
                .build();
    }

    /**
     * The standard double options. Allows missing and accepts {@link JsonValueTypes#numberOrNull()}.
     *
     * @return the standard double options
     */
    public static DoubleOptions standard() {
        return builder().build();
    }

    /**
     * The strict double options. Disallows missing and accepts {@link JsonValueTypes#number()}.
     *
     * @return the strict double options
     */
    public static DoubleOptions strict() {
        return builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.number())
                .build();
    }

    /**
     * {@inheritDoc} By default is {@link JsonValueTypes#numberOrNull()}.
     */
    @Override
    @Default
    public Set<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.numberOrNull();
    }

    /**
     * {@inheritDoc} Is {@link JsonValueTypes#numberLike()}.
     */
    @Override
    public final Set<JsonValueTypes> universe() {
        return JsonValueTypes.numberLike();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptionsSingleValueBase.Builder<Double, DoubleOptions, Builder> {

        Builder onNull(double onNull);

        Builder onMissing(double onMissing);

        default Builder onNull(Double onNull) {
            return onNull == null ? this : onNull((double) onNull);
        }

        default Builder onMissing(Double onMissing) {
            return onMissing == null ? this : onMissing((double) onMissing);
        }
    }
}
