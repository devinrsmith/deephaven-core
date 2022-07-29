/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api;

import io.deephaven.api.agg.Pair;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterAnd;
import io.deephaven.api.filter.FilterCondition;
import io.deephaven.api.filter.FilterIsNotNull;
import io.deephaven.api.filter.FilterIsNull;
import io.deephaven.api.filter.FilterNot;
import io.deephaven.api.filter.FilterOr;
import io.deephaven.api.value.Value;

import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A set of static helpers to turn strongly-typed api arguments into their {@link String} counterparts.
 */
public class Strings {

    public static String of(ColumnName columnName) {
        return columnName.name();
    }

    public static String of(RawString rawString) {
        return rawString.value();
    }

    public static String of(FilterCondition condition) {
        String lhs = of(condition.lhs());
        String rhs = of(condition.rhs());
        switch (condition.operator()) {
            case LESS_THAN:
                return String.format("%s < %s", lhs, rhs);
            case LESS_THAN_OR_EQUAL:
                return String.format("%s <= %s", lhs, rhs);
            case GREATER_THAN:
                return String.format("%s > %s", lhs, rhs);
            case GREATER_THAN_OR_EQUAL:
                return String.format("%s >= %s", lhs, rhs);
            case EQUALS:
                return String.format("%s == %s", lhs, rhs);
            case NOT_EQUALS:
                return String.format("%s != %s", lhs, rhs);
            default:
                throw new IllegalStateException(
                        "Unexpected condition operator: " + condition.operator());
        }
    }

    public static String of(FilterNot not) {
        return String.format("!(%s)", of(not.filter()));
    }

    public static String of(FilterIsNull isNull) {
        return String.format("isNull(%s)", of(isNull.column()));
    }

    public static String of(FilterIsNotNull isNotNull) {
        return String.format("!isNull(%s)", of(isNotNull.column()));
    }

    public static String of(FilterOr filterOr) {
        return filterOr.filters().stream().map(Strings::of)
                .collect(Collectors.joining(") || (", "(", ")"));
    }

    public static String of(FilterAnd filterAnd) {
        return filterAnd.filters().stream().map(Strings::of)
                .collect(Collectors.joining(") && (", "(", ")"));
    }

    public static String of(Pair pair) {
        if (pair.input().equals(pair.output())) {
            return of(pair.output());
        }
        return String.format("%s=%s", of(pair.output()), of(pair.input()));
    }

    public static String of(JoinMatch match) {
        if (match.left().equals(match.right())) {
            return of(match.left());
        }
        return String.format("%s==%s", of(match.left()), of(match.right()));
    }

    public static String of(JoinAddition addition) {
        if (addition.newColumn().equals(addition.existingColumn())) {
            return of(addition.newColumn());
        }
        return String.format("%s=%s", of(addition.newColumn()), of(addition.existingColumn()));
    }

    public static String of(Selectable selectable) {
        String lhs = of(selectable.newColumn());
        if (selectable.newColumn().equals(selectable.expression())) {
            return lhs;
        }
        String rhs = selectable.expression().walk(UniversalAdapter.INSTANCE);
        return String.format("%s=%s", lhs, rhs);
    }

    public static String of(Expression expression) {
        return expression.walk(UniversalAdapter.INSTANCE);
    }

    public static String of(Filter filter) {
        return filter.walk(UniversalAdapter.INSTANCE);
    }

    public static String of(Value value) {
        return value.walk((Value.Visitor<String>) UniversalAdapter.INSTANCE);
    }

    /**
     * If we ever need to provide more specificity for a type, we can create a non-universal impl.
     */
    private enum UniversalAdapter
            implements Filter.Visitor<String>, Expression.Visitor<String>, Value.Visitor<String> {
        INSTANCE;

        @Override
        public String visit(ColumnName name) {
            return of(name);
        }

        @Override
        public String visit(RawString rawString) {
            return of(rawString);
        }

        @Override
        public String visit(FilterCondition condition) {
            return of(condition);
        }

        @Override
        public String visit(FilterIsNull isNull) {
            return of(isNull);
        }

        @Override
        public String visit(FilterIsNotNull isNotNull) {
            return of(isNotNull);
        }

        @Override
        public String visit(FilterNot not) {
            return of(not);
        }

        @Override
        public String visit(FilterOr ors) {
            return of(ors);
        }

        @Override
        public String visit(FilterAnd ands) {
            return of(ands);
        }

        @Override
        public String visit(Value value) {
            return of(value);
        }

        @Override
        public String visit(long x) {
            return Long.toString(x);
        }
    }
}
