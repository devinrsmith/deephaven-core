package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Selectable;
import io.deephaven.api.expression.ExpressionVisitorDelegate;
import io.deephaven.engine.table.impl.strings.ExpressionString;

import java.util.Objects;

class SelectColumnAdapter extends ExpressionVisitorDelegate<String, SelectColumn> {

    public static SelectColumn of(Selectable selectable) {
        return selectable.expression().walk(new SelectColumnAdapter(selectable.newColumn()));
    }

    private final ColumnName lhs;

    private SelectColumnAdapter(ColumnName lhs) {
        super(ExpressionString.INSTANCE);
        this.lhs = Objects.requireNonNull(lhs);
    }

    @Override
    public SelectColumn visit(ColumnName rhs) {
        return new SourceColumn(rhs.name(), lhs.name());
    }

    @Override
    public SelectColumn adapt(String rhs) {
        // TODO(deephaven-core#3740): Remove engine crutch on io.deephaven.api.Strings
        return SelectColumnFactory.getExpression(lhs.name() + "=" + rhs);
    }
}
