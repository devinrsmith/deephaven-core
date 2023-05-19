package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.Method;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterAnd;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.filter.FilterComparison.Operator;
import io.deephaven.api.filter.FilterIn;
import io.deephaven.api.filter.FilterIsNull;
import io.deephaven.api.filter.FilterOr;
import io.deephaven.api.filter.FilterPattern;
import io.deephaven.api.literal.Literal;
import io.deephaven.engine.table.impl.strings.FilterString;

import java.util.stream.Collectors;

enum WhereFilterAdapter implements Filter.SimplifiedVisitor<WhereFilter> {
    INSTANCE;

    public static WhereFilter of(Filter filter) {
        return filter.walk(INSTANCE);
    }

    @Override
    public WhereFilter visit(FilterOr ors) {
        // A || B || ... || Z
        return DisjunctiveFilter.of(ors);
    }

    @Override
    public WhereFilter visit(FilterAnd ands) {
        // A && B && ... && Z
        return ConjunctiveFilter.of(ands);
    }

    @Override
    public WhereFilter visit(FilterComparison comparison) {
        return WhereFilterComparisonAdapter.of(comparison);
    }

    @Override
    public WhereFilter visit(FilterPattern pattern, boolean inverted) {
        final WhereFilter filter = WhereFilterPatternImpl.of(pattern);
        return inverted ? WhereFilterInvertedImpl.of(filter) : filter;
    }

    @Override
    public WhereFilter visit(FilterIsNull isNull, boolean inverted) {
        return WhereFilterIsNullAdapter.of(isNull.expression(), inverted);
    }

    @Override
    public WhereFilter visit(FilterIn in, boolean inverted) {
        if (in.values().size() == 1) {
            // Simplified case, handles transpositions of LHS / RHS most optimally
            final Operator operator = inverted ? Operator.NOT_EQUALS : Operator.EQUALS;
            final FilterComparison comparison = operator.of(in.expression(), in.values().get(0));
            return visit(comparison);
        }
        if (in.expression() instanceof ColumnName) {
            // In the case where LHS is a column name, we want to be as efficient as possible and only read that column
            // data once. MatchFilter allows us to do that.
            if (in.values().stream().allMatch(p -> p instanceof Literal)) {
                return MatchFilter.ofLiterals(
                        ((ColumnName) in.expression()).name(),
                        in.values().stream().map(Literal.class::cast).collect(Collectors.toList()),
                        inverted);
            }
            // It would be nice if there was a way to allow efficient read access with Disjunctive / Conjunctive
            // constructions. Otherwise, we fall back to Condition filter.
            // TODO(deephaven-core#3791): Non-vectorized version of Disjunctive / Conjunctive filters
            return whereFilter(inverted
                    ? FilterString.INSTANCE.visit(invertedAnds(in))
                    : FilterString.INSTANCE.visit(regularOrs(in)));
        }
        return inverted ? visit(invertedAnds(in)) : visit(regularOrs(in));
    }

    @Override
    public WhereFilter visit(Function function, boolean inverted) {
        return whereFilter(FilterString.INSTANCE.visit(function, inverted));
    }

    @Override
    public WhereFilter visit(Method method, boolean inverted) {
        return whereFilter(FilterString.INSTANCE.visit(method, inverted));
    }

    @Override
    public WhereFilter visit(RawString rawString, boolean inverted) {
        return whereFilter(FilterString.INSTANCE.visit(rawString, inverted));
    }

    @Override
    public WhereFilter visit(boolean literal) {
        throw new UnsupportedOperationException("Should not be constructing literal boolean WhereFilter");
    }

    private static WhereFilter whereFilter(String x) {
        return WhereFilterFactory.getExpression(x);
    }

    private static FilterOr regularOrs(FilterIn in) {
        final FilterOr.Builder builder = FilterOr.builder();
        final Expression exp = in.expression();
        for (Expression value : in.values()) {
            builder.addFilters(FilterComparison.eq(exp, value));
        }
        return builder.build();
    }

    private static FilterAnd invertedAnds(FilterIn in) {
        final FilterAnd.Builder builder = FilterAnd.builder();
        final Expression exp = in.expression();
        for (Expression value : in.values()) {
            builder.addFilters(FilterComparison.neq(exp, value));
        }
        return builder.build();
    }
}
