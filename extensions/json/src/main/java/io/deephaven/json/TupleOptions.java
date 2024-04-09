//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Processes a JSON array as an tuple.
 */
@Immutable
@BuildableStyle
public abstract class TupleOptions extends ValueOptions {

    public static Builder builder() {
        return ImmutableTupleOptions.builder();
    }

    // todo: allow users to specify the indices they care about; maybe w/ Skip?
    public static TupleOptions of(ValueOptions... values) {
        return builder().addValues(values).build();
    }

    public static TupleOptions of(Iterable<? extends ValueOptions> values) {
        return builder().addAllValues(values).build();
    }

    public abstract List<ValueOptions> values();

    // todo
    @Override
    public final EnumSet<JsonValueTypes> allowedTypes() {
        return values().stream().allMatch(valueOptions -> valueOptions.allowedTypes().contains(JsonValueTypes.NULL))
                ? JsonValueTypes.ARRAY_OR_NULL
                : EnumSet.of(JsonValueTypes.ARRAY);
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

    @Override
    final EnumSet<JsonValueTypes> restrictedToTypes() {
        return JsonValueTypes.ARRAY_OR_NULL;
    }
}
