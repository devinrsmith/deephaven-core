package io.deephaven.api.expression;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.expression.Expression.Visitor;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.literal.Literal;

import java.util.Objects;

public abstract class ExpressionVisitorDelegate<T, R> implements Expression.Visitor<R> {
    private final Expression.Visitor<T> delegate;

    public ExpressionVisitorDelegate(Visitor<T> delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    public abstract R adapt(T t);

    @Override
    public R visit(ColumnName columnName) {
        return adapt(delegate.visit(columnName));
    }

    @Override
    public R visit(Literal literal) {
        return adapt(delegate.visit(literal));
    }

    @Override
    public R visit(Filter filter) {
        return adapt(delegate.visit(filter));
    }

    @Override
    public R visit(Function function) {
        return this.adapt(delegate.visit(function));
    }

    @Override
    public R visit(Method method) {
        return adapt(delegate.visit(method));
    }

    @Override
    public R visit(RawString rawString) {
        return adapt(delegate.visit(rawString));
    }
}
