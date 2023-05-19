package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.expression.ExpressionVisitorDefault;
import io.deephaven.api.expression.ExpressionVisitorDelegate;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.literal.AsObject;
import io.deephaven.api.literal.Literal;
import io.deephaven.engine.table.impl.select.MatchFilter.MatchType;
import io.deephaven.engine.table.impl.strings.ExpressionString;
import io.deephaven.gui.table.filters.Condition;

import java.util.Objects;

class WhereFilterComparisonAdapter extends ExpressionVisitorDelegate<String, WhereFilter> {

    public static WhereFilter of(FilterComparison condition) {
        FilterComparison preferred = condition.maybeTranspose();
        return preferred.lhs().walk(new WhereFilterComparisonAdapter(condition, preferred));
    }

    private final FilterComparison original;
    private final FilterComparison preferred;

    private WhereFilterComparisonAdapter(FilterComparison original, FilterComparison preferred) {
        super(ExpressionString.INSTANCE);
        this.original = Objects.requireNonNull(original);
        this.preferred = Objects.requireNonNull(preferred);
    }

    @Override
    public WhereFilter visit(ColumnName lhs) {
        final WhereFilter filter = preferred.rhs().walk(new RhsVisitor(lhs));
        return filter != null ? filter : super.visit(lhs);
    }

    @Override
    public WhereFilter adapt(String s) {
        return WhereFilterFactory.getExpression(s);
    }

    private class RhsVisitor extends ExpressionVisitorDefault<WhereFilter> {
        private final ColumnName lhs;

        public RhsVisitor(ColumnName lhs) {
            super(null);
            this.lhs = Objects.requireNonNull(lhs);
        }

        @Override
        public WhereFilter visit(Literal value) {
            final Object rhsObj = AsObject.of(value);
            switch (preferred.operator()) {
                case EQUALS:
                    return new MatchFilter(lhs.name(), rhsObj);
                case NOT_EQUALS:
                    return new MatchFilter(MatchType.Inverted, lhs.name(), rhsObj);
                case LESS_THAN:
                    return new RangeConditionFilter(lhs.name(), Condition.LESS_THAN, rhsObj.toString());
                case LESS_THAN_OR_EQUAL:
                    return new RangeConditionFilter(lhs.name(), Condition.LESS_THAN_OR_EQUAL, rhsObj.toString());
                case GREATER_THAN:
                    return new RangeConditionFilter(lhs.name(), Condition.GREATER_THAN, rhsObj.toString());
                case GREATER_THAN_OR_EQUAL:
                    return new RangeConditionFilter(lhs.name(), Condition.GREATER_THAN_OR_EQUAL, rhsObj.toString());
                default:
                    throw new IllegalStateException("Unexpected operator " + original.operator());
            }
        }
    }
}
