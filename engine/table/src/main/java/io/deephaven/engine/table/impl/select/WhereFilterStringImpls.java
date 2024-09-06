//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.Method;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterAnd;
import io.deephaven.api.filter.FilterContains;
import io.deephaven.api.filter.FilterEndsWith;
import io.deephaven.api.filter.FilterStartsWith;
import io.deephaven.api.literal.Literal;

public final class WhereFilterStringImpls {
    // public static WhereFilter startsWith(ColumnName target, String prefix, boolean invert) {
    // return ConditionFilter.createConditionFilter(String.format("!isNull(%s) && %s%s.startsWith(`%s`)", target.name(),
    // invert ? "!" : "", target.name(), prefix));
    // }
    //
    // public static WhereFilter endsWith(ColumnName target, String suffix, boolean invert) {
    // return ConditionFilter.createConditionFilter(String.format("!isNull(%s) && %s%s.endsWith(`%s`)", target.name(),
    // invert ? "!" : "", target.name(), suffix));
    // }

    public static WhereFilter contains(FilterContains contains) {
        if (!(contains.expression() instanceof ColumnName)) {
            throw new IllegalArgumentException("contains only supports filtering against a column name");
        }
        if (!contains.caseSensitive()) {
            // When case insensitive, fallback to FilterPattern contains
            return WhereFilterPatternImpl.of(contains.toFilterPattern());
        }
        final String columnName = ((ColumnName) contains.expression()).name();
        // todo: escape sequence
        return ConditionFilter.createConditionFilter(String.format("!isNull(%s) && %s%s.contains(`%s`)", columnName,
                contains.invertPattern() ? "!" : "", columnName, contains.sequence()));
    }

    public static WhereFilter startsWith(FilterStartsWith startsWith) {
        return WhereFilterAdapter.of(simplify(startsWith));
    }

    private static FilterAnd simplify(FilterStartsWith startsWith) {
        final Expression expression = startsWith.expression();
        if (!(expression instanceof ColumnName)) {
            throw new IllegalArgumentException("startsWith only supports filtering against a column name");
        }
        final String prefix = startsWith.prefix();
        Filter method = startsWith.caseSensitive()
                ? Method.of(expression, "startsWith", Literal.of(prefix))
                : Method.of(expression, "regionMatches", Literal.of(true), Literal.of(0), Literal.of(prefix), Literal.of(0), Literal.of(prefix.length()));
        if (startsWith.invertPattern()) {
            method = method.invert();
        }
        return FilterAnd.of(Filter.isNotNull(expression), method);
    }

    private static FilterAnd simplify(FilterEndsWith endsWith) {
        final Expression expression = endsWith.expression();
        if (!(expression instanceof ColumnName)) {
            throw new IllegalArgumentException("endsWith only supports filtering against a column name");
        }
        final String prefix = endsWith.suffix();
        final Expression expressionLength = Method.of(expression, "length");
        final Literal prefixLength = Literal.of(prefix.length());
        final Expression expressionOffset = Function.of("io.deephaven.engine.table.impl.select.WhereFilterStringImpls.minus", expressionLength, prefixLength);
        Filter method = endsWith.caseSensitive()
                ? Method.of(expression, "endsWith", Literal.of(prefix))
//                : Function.of("io.deephaven.engine.table.impl.select.WhereFilterStringImpls.endsWithCaseInsensitive", expression, Literal.of(prefix));
                : Method.of(expression, "regionMatches", Literal.of(true), expressionOffset, Literal.of(prefix), Literal.of(0), prefixLength);
        if (endsWith.invertPattern()) {
            method = method.invert();
        }
        return FilterAnd.of(Filter.isNotNull(expression), method);
    }

//    public static boolean startsWithCaseInsensitive(String target, String suffix) {
//        return target.regionMatches(true, 0, suffix, 0, suffix.length());
//    }
//
//    public static boolean endsWithCaseInsensitive(String target, String suffix) {
//        final int suffixLength = suffix.length();
//        return target.regionMatches(true, target.length() - suffixLength, suffix, 0, suffixLength);
//    }

    public static int minus(int x, int y) {
        return x - y;
    }
}
