/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.util.List;

@Immutable
@BuildableStyle
public abstract class TupleOptions extends ValueOptions {

    public static Builder builder() {
        return ImmutableTupleOptions.builder();
    }

    public static TupleOptions of(ValueOptions... values) {
        return builder().addValues(values).build();
    }

    public static TupleOptions of(Iterable<? extends ValueOptions> values) {
        return builder().addAllValues(values).build();
    }

    public abstract List<ValueOptions> values();

    @Override
    public final boolean allowNull() {
        return values().stream().allMatch(ValueOptions::allowNull);
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
