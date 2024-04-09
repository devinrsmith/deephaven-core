//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.EnumSet;
import java.util.OptionalInt;

/**
 * Processes a JSON value as an {@code int}.
 */
@Immutable
@BuildableStyle
public abstract class IntOptions extends ValueOptionsRestrictedUniverseBase {

    public static Builder builder() {
        return ImmutableIntOptions.builder();
    }

    /**
     * The lenient int options.
     *
     * @return the lenient int options
     */
    public static IntOptions lenient() {
        return builder()
                .allowedTypes(JsonValueTypes.INT_LIKE)
                .build();
    }

    /**
     * The standard int options.
     *
     * @return the standard int options
     */
    public static IntOptions standard() {
        return builder().build();
    }

    /**
     * The strict int options.
     *
     * @return the strict int options
     */
    public static IntOptions strict() {
        return builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.INT)
                .build();
    }

    /**
     * {@inheritDoc} By default is {@link JsonValueTypes#INT_OR_NULL}.
     */
    @Default
    @Override
    public EnumSet<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.INT_OR_NULL;
    }

    /**
     * The universe, is {@link JsonValueTypes#NUMBER_LIKE}.
     */
    @Override
    public final EnumSet<JsonValueTypes> universe() {
        return JsonValueTypes.NUMBER_LIKE;
    }

    /**
     * The on-null value.
     */
    public abstract OptionalInt onNull();

    /**
     * The on-missing value.
     */
    public abstract OptionalInt onMissing();


    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<IntOptions, Builder> {

        Builder onNull(int onNull);

        Builder onMissing(int onMissing);
    }

    @Check
    final void checkOnNull() {
        if (!allowedTypes().contains(JsonValueTypes.NULL) && onNull().isPresent()) {
            throw new IllegalArgumentException("onNull set, but NULL is not allowed");
        }
    }

    @Check
    final void checkOnMissing() {
        if (!allowMissing() && onMissing().isPresent()) {
            throw new IllegalArgumentException("onMissing set, but allowMissing is false");
        }
    }
}
