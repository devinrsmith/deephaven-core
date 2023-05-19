package io.deephaven.api.expression;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.expression.Expression.Visitor;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.literal.Literal;

public class ExpressionVisitorDefault<T> implements Visitor<T> {

    private final T defaultValue;

    public ExpressionVisitorDefault(T defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Override
    public T visit(Literal literal) {
        return defaultValue;
    }

    @Override
    public T visit(ColumnName columnName) {
        return defaultValue;
    }

    @Override
    public T visit(Filter filter) {
        return defaultValue;
    }

    @Override
    public T visit(Function function) {
        return defaultValue;
    }

    @Override
    public T visit(Method method) {
        return defaultValue;
    }

    @Override
    public T visit(RawString rawString) {
        return defaultValue;
    }
}
