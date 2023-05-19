package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.expression.ExpressionVisitorDelegate;
import io.deephaven.engine.table.impl.select.MatchFilter.MatchType;
import io.deephaven.engine.table.impl.strings.ExpressionString;

class WhereFilterIsNullAdapter extends ExpressionVisitorDelegate<String, WhereFilter> {

    public static WhereFilter of(Expression expression, boolean inverted) {
        return expression.walk(new WhereFilterIsNullAdapter(inverted));
    }

    private final boolean inverted;

    private WhereFilterIsNullAdapter(boolean inverted) {
        super(ExpressionString.INSTANCE);
        this.inverted = inverted;
    }

    @Override
    public WhereFilter visit(ColumnName columnName) {
        return new MatchFilter(
                inverted ? MatchType.Inverted : MatchType.Regular,
                columnName.name(),
                new Object[] {null});
    }

    @Override
    public WhereFilter adapt(String s) {
        return WhereFilterFactory.getExpression((inverted ? "!isNull(" : "isNull(") + s + ")");
    }
}
