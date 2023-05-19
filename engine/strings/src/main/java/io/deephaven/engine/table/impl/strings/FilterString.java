package io.deephaven.engine.table.impl.strings;

import io.deephaven.api.RawString;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.Method;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterAnd;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.filter.FilterIn;
import io.deephaven.api.filter.FilterIsNull;
import io.deephaven.api.filter.FilterOr;
import io.deephaven.api.filter.FilterPattern;

import java.util.Iterator;
import java.util.stream.Collectors;

/**
 * @see io.deephaven.engine.table.impl.strings
 */
public enum FilterString implements Filter.SimplifiedVisitor<String> {
    INSTANCE;

    public static String of(Filter filter) {
        return filter.walk(INSTANCE);
    }

    @Override
    public String visit(FilterComparison fc) {
        final String lhs = ExpressionString.of(fc.lhs());
        final String rhs = ExpressionString.of(fc.rhs());
        return "(" + lhs + ") " + fc.operator().javaOperator() + " (" + rhs + ")";
    }

    @Override
    public String visit(FilterOr ors) {
        return ors.filters().stream().map(FilterString::of).collect(Collectors.joining(") || ("));
    }

    @Override
    public String visit(FilterAnd ands) {
        return ands.filters().stream().map(FilterString::of).collect(Collectors.joining(") && ("));
    }

    @Override
    public String visit(boolean literal) {
        return Boolean.toString(literal);
    }

    @Override
    public String visit(FilterIsNull isNull, boolean inverted) {
        return (inverted ? "!" : "") + of(isNull);
    }

    @Override
    public String visit(FilterIn in, boolean inverted) {
        final StringBuilder sb = new StringBuilder();
        final String lhs = ExpressionString.of(in.expression());
        sb.append(lhs);
        if (inverted) {
            sb.append(" not in ");
        } else {
            sb.append(" in ");
        }
        final Iterator<Expression> it = in.values().iterator();
        sb.append(ExpressionString.of(it.next()));
        while (it.hasNext()) {
            sb.append(", ");
            sb.append(ExpressionString.of(it.next()));
        }
        return sb.toString();
    }

    @Override
    public String visit(FilterPattern pattern, boolean inverted) {
        throw new UnsupportedEngineString(pattern);
    }

    @Override
    public String visit(Function function, boolean inverted) {
        return (inverted ? "!" : "") + Impl.of(function);
    }

    @Override
    public String visit(Method method, boolean inverted) {
        return (inverted ? "!" : "") + Impl.of(method);
    }

    @Override
    public String visit(RawString rawString, boolean inverted) {
        final String inner = Impl.of(rawString);
        if (inverted) {
            return "!(" + inner + ")";
        }
        return inner;
    }
}
