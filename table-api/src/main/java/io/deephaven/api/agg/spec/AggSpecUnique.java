/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.agg.spec;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Specifies an aggregation that outputs the single unique input value for groups that have one, {@code null} if all
 * input values are {@code null}, or {@link #nonUniqueSentinel()} if there is more than one distinct value.
 */
@Immutable
@BuildableStyle
public abstract class AggSpecUnique extends AggSpecBase {

    public static final boolean INCLUDE_NULLS_DEFAULT = false;

    public static Builder builder() {
        return ImmutableAggSpecUnique.builder();
    }

    /**
     * Specify a "unique" aggregation that does not treat {@code null} as a value for purposes of determining if the
     * values in a group are unique. If a group is non-empty but contains only {@code null} values, its result will be
     * {@code null}. If a group contains more than a single unique value, its result will also be {@code null}.
     * 
     * @return The "unique" aggregation specification
     */
    public static AggSpecUnique of() {
        return builder().build();
    }

    /**
     * Specify a "unique" aggregation that optionally treats {@code null} as a value for purposes of determining if the
     * values in a group are unique. If a group is non-empty but contains only {@code null} values, its result will be
     * {@code null}. If a group contains more than a single unique value, its result will be {@code nonUniqueSentinel}.
     *
     * @param includeNulls Whether {@code null} is treated as a value for determining if the values in a group are
     *        unique
     * @param nonUniqueSentinel Sentinel value to use if a group contains more than a single unique value
     * @return The "unique" aggregation specification
     */
    public static AggSpecUnique of(boolean includeNulls, Object nonUniqueSentinel) {
        AggSpecUnique.Builder builder = builder().includeNulls(includeNulls);
        if (nonUniqueSentinel != null) {
            builder.nonUniqueSentinel(nonUniqueSentinel);
        }
        return builder().build();
    }

    @Override
    public final String description() {
        return "unique" + (includeNullsOrDefault() ? " (including nulls)" : "");
    }

    /**
     * Whether to include {@code null} values as a distinct value for determining if there is only one unique value to
     * output.
     *
     * @return Whether to include nulls
     */
    @Nullable
    public abstract Boolean includeNulls();

    /**
     * The output value to use for groups that don't have a single unique input value.
     *
     * @return The non-unique sentinel value
     */
    public abstract Optional<Object> nonUniqueSentinel();

    public final boolean includeNullsOrDefault() {
        final Boolean includeNulls = includeNulls();
        return includeNulls == null ? INCLUDE_NULLS_DEFAULT : includeNulls;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder {
        Builder includeNulls(Boolean includeNulls);

        Builder nonUniqueSentinel(Object nonUniqueSentinel);

        AggSpecUnique build();
    }
}
