package io.deephaven.engine.table.impl.strings;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.Method;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.literal.Literal;

/**
 * @see io.deephaven.engine.table.impl.strings
 */
public enum ExpressionString implements Expression.Visitor<String> {
    INSTANCE;

    public static String of(Expression expression) {
        return expression.walk(INSTANCE);
    }

    @Override
    public String visit(ColumnName columnName) {
        return columnName.name();
    }

    @Override
    public String visit(Literal literal) {
        return LiteralString.of(literal);
    }

    @Override
    public String visit(Filter filter) {
        return FilterString.of(filter);
    }

    @Override
    public String visit(Function function) {
        return Impl.of(function);
    }

    @Override
    public String visit(Method method) {
        return Impl.of(method);
    }

    @Override
    public String visit(RawString rawString) {
        return Impl.of(rawString);
    }
}
