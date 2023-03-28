/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.expression;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.literal.Literal;
import org.junit.jupiter.api.Test;

import static io.deephaven.api.Strings.of;
import static io.deephaven.api.filter.Filter.and;
import static io.deephaven.api.filter.Filter.isNotNull;
import static io.deephaven.api.filter.Filter.isNull;
import static io.deephaven.api.filter.Filter.ofFalse;
import static io.deephaven.api.filter.Filter.ofTrue;
import static io.deephaven.api.filter.Filter.or;
import static io.deephaven.api.filter.FilterComparison.gt;
import static org.assertj.core.api.Assertions.assertThat;

public class ExpressionTest {

    public static final ColumnName FOO = ColumnName.of("Foo");
    public static final ColumnName BAR = ColumnName.of("Bar");
    public static final ColumnName BAZ = ColumnName.of("Baz");

    @Test
    void columnName() {
        toString(FOO, "Foo");
    }

    @Test
    void filter() {
        toString(or(gt(FOO, BAR), gt(FOO, BAZ), and(isNull(FOO), isNotNull(BAR), isNotNull(BAZ))),
                "(Foo > Bar) || (Foo > Baz) || (isNull(Foo) && !isNull(Bar) && !isNull(Baz))");
    }

    @Test
    void expressionFunction() {
        toString(f("plus", FOO, BAR), "plus(Foo, Bar)");
        toString(f("plus", FOO, f("minus", BAR, BAZ)), "plus(Foo, minus(Bar, Baz))");
    }

    @Test
    void expressionFunctionThatTakesFilters() {
        toString(f("some_func", gt(FOO, BAR), BAZ, ofTrue(), ofFalse(), and(isNull(FOO), isNotNull(BAR))),
                "some_func(Foo > Bar, Baz, true, false, isNull(Foo) && !isNull(Bar))");
    }

    @Test
    void literals() {
        toString(Literal.of(true), "true");
        toString(Literal.of(false), "false");
        toString(Literal.of(42), "(int)42");
        toString(Literal.of(42L), "42L");
    }

    @Test
    void rawString() {
        toString(RawString.of("Foo + Bar - 42"), "Foo + Bar - 42");
    }

    private static void toString(Expression expression, String expected) {
        assertThat(of(expression)).isEqualTo(expected);
        assertThat(expression.walk(SpecificMethod.INSTANCE)).isEqualTo(expected);
    }

    private static ExpressionFunction f(String name, Expression... expressions) {
        return ExpressionFunction.builder().name(name).addArguments(expressions).build();
    }

    private enum SpecificMethod implements Expression.Visitor<String> {
        INSTANCE;

        @Override
        public String visit(Literal literal) {
            return of(literal);
        }

        @Override
        public String visit(ColumnName columnName) {
            return of(columnName);
        }

        @Override
        public String visit(Filter filter) {
            return of(filter);
        }

        @Override
        public String visit(ExpressionFunction function) {
            return of(function);
        }

        @Override
        public String visit(RawString rawString) {
            return of(rawString);
        }
    }
}
