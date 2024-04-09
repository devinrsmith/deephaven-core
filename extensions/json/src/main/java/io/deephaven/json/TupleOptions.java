//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.util.EnumSet;
import java.util.List;

/**
 * Processes a JSON array as a tuple.
 */
@Immutable
@BuildableStyle
public abstract class TupleOptions extends ValueOptionsRestrictedUniverseBase {

    public static Builder builder() {
        return ImmutableTupleOptions.builder();
    }

    public static TupleOptions of(ValueOptions... values) {
        return builder().addValues(values).build();
    }

    public static TupleOptions of(Iterable<? extends ValueOptions> values) {
        return builder().addAllValues(values).build();
    }

    /**
     * The ordered values of the tuple.
     */
    public abstract List<ValueOptions> values();

    /**
     * {@inheritDoc} By default is {@link JsonValueTypes#ARRAY_OR_NULL} when all the the {@link #values()} allow
     * {@link JsonValueTypes#NULL}, otherwise defaults to {@link JsonValueTypes#ARRAY}.
     */
    @Override
    public EnumSet<JsonValueTypes> allowedTypes() {
        return values().stream().allMatch(valueOptions -> valueOptions.allowedTypes().contains(JsonValueTypes.NULL))
                ? JsonValueTypes.ARRAY_OR_NULL
                : EnumSet.of(JsonValueTypes.ARRAY);
    }

    /**
     * The universe, is {@link JsonValueTypes#ARRAY_OR_NULL}.
     */
    @Override
    public final EnumSet<JsonValueTypes> universe() {
        return JsonValueTypes.ARRAY_OR_NULL;
    }

    @Override
    public final boolean allowMissing() {
        return values().stream().allMatch(ValueOptions::allowMissing);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    // Note: Builder does not extend ValueOptions.Builder b/c allowNull / allowMissing is implicitly set

    public interface Builder {

        Builder addValues(ValueOptions element);

        Builder addValues(ValueOptions... elements);

        Builder addAllValues(Iterable<? extends ValueOptions> elements);

        TupleOptions build();
    }
}
