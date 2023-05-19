/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.filter;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.RawString;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.Method;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.Objects;

/**
 * Evaluates to {@code true} when the given {@link #filter() filter} evaluates to {@code false}.
 */
@Immutable
@SimpleStyle
public abstract class FilterNot<F extends Filter> extends FilterBase {

    public static <F extends Filter> FilterNot<F> of(F filter) {
        return ImmutableFilterNot.of(filter);
    }

    /**
     * The filter.
     *
     * @return the filter
     */
    @Parameter
    public abstract F filter();

    /**
     * Equivalent to {@code filter()}.
     *
     * @return the inverse filter
     */
    @Override
    public final F invert() {
        return filter();
    }

    /**
     * Creates a logical equivalent of {@code this} equal to {@code filter().inverse()}. It's possible that the result
     * is equal to {@code this}.
     *
     * @return the inverted filter
     */
    public final Filter invertFilter() {
        return filter().invert();
    }

    @Override
    public final <T> T walk(Filter.Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public final <T> T walk(SimplifiedVisitor<T> visitor) {
        return filter().walk(new NotSimplifier<>(visitor));
    }

    private static class NotSimplifier<T> implements Visitor<T> {
        private final SimplifiedVisitor<T> visitor;
        private boolean inverted;

        public NotSimplifier(SimplifiedVisitor<T> visitor) {
            this.visitor = Objects.requireNonNull(visitor);
            this.inverted = true;
        }

        @Override
        public T visit(FilterNot<?> not) {
            inverted = !inverted;
            return not.filter().walk(this);
        }

        @Override
        public T visit(FilterComparison comparison) {
            return inverted ? visitor.visit(comparison.invert()) : visitor.visit(comparison);
        }

        @Override
        public T visit(FilterOr ors) {
            return inverted ? visitor.visit(ors.invert()) : visitor.visit(ors);
        }

        @Override
        public T visit(FilterAnd ands) {
            return inverted ? visitor.visit(ands.invert()) : visitor.visit(ands);
        }

        @Override
        public T visit(boolean literal) {
            return visitor.visit(literal ^ inverted);
        }

        @Override
        public T visit(FilterIsNull isNull) {
            return visitor.visit(isNull, inverted);
        }

        @Override
        public T visit(FilterIn in) {
            return visitor.visit(in, inverted);
        }

        @Override
        public T visit(FilterPattern pattern) {
            return visitor.visit(pattern, inverted);
        }

        @Override
        public T visit(Function function) {
            return visitor.visit(function, inverted);
        }

        @Override
        public T visit(Method method) {
            return visitor.visit(method, inverted);
        }

        @Override
        public T visit(RawString rawString) {
            return visitor.visit(rawString, inverted);
        }
    }
}
