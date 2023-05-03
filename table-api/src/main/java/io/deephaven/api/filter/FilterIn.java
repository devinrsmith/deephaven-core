package io.deephaven.api.filter;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.expression.Expression;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Evaluates to true when {@link #expression()} is equal to any value in {@link #values()}.
 *
 * <p>
 * See {@link #asComparisons()} for the logical equivalent expressed more verbosely.
 */
@Immutable
@BuildableStyle
public abstract class FilterIn extends FilterBase {

    public static Builder builder() {
        return ImmutableFilterIn.builder();
    }

    public static FilterIn of(Expression expression, List<Expression> values) {
        return builder().expression(expression).addAllValues(values).build();
    }

    public static FilterIn of(Expression expression, Expression... values) {
        return builder().expression(expression).addValues(values).build();
    }

    public abstract Expression expression();

    public abstract List<Expression> values();

    @Override
    public final FilterNot<FilterIn> invert() {
        return FilterNot.of(this);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    /**
     * Creates the logical equivalent of {@code this} as an {@link Filter#or(Collection)} with
     * {@link FilterComparison#eq(Expression, Expression)} between {@link #expression()} and each {@link #values()}.
     *
     * <p>
     * Equivalent to
     * {@code Filter.or(values().stream().map(rhs -> FilterComparison.eq(expression(), rhs)).collect(Collectors.toList()))}.
     *
     * @return the filter as comparisons
     * @see Filter#or(Collection)
     * @see FilterComparison#eq(Expression, Expression)
     */
    public final Filter asComparisons() {
        return Filter.or(values().stream().map(this::expEq).collect(Collectors.toList()));
    }

    /**
     * Creates the logical equivalent of {@code not(this)} as an {@link Filter#and(Collection)} with
     * {@link FilterComparison#neq(Expression, Expression)} between {@link #expression()} and each {@link #values()}.
     *
     * <p>
     * Equivalent to
     * {@code Filter.and(values().stream().map(rhs -> FilterComparison.neq(expression(), rhs)).collect(Collectors.toList()))}.
     *
     * @return the inverse filter as comparisons
     * @see Filter#and(Collection)
     * @see FilterComparison#neq(Expression, Expression)
     */
    public final Filter inverseAsComparisons() {
        return Filter.and(values().stream().map(this::expNeq).collect(Collectors.toList()));
    }

    @Check
    final void checkNotEmpty() {
        if (values().isEmpty()) {
            throw new IllegalArgumentException("FilterIn.values() must be non-empty");
        }
    }

    private FilterComparison expEq(Expression rhs) {
        return FilterComparison.eq(expression(), rhs);
    }

    private FilterComparison expNeq(Expression rhs) {
        return FilterComparison.neq(expression(), rhs);
    }

    public interface Builder {
        Builder expression(Expression expression);

        Builder addValues(Expression elements);

        Builder addValues(Expression... elements);

        Builder addAllValues(Iterable<? extends Expression> elements);

        FilterIn build();
    }
}