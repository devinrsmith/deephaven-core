package io.deephaven.qst.table;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class RawString implements Expression, Filter {

    @Parameter
    public abstract String value();

    @Override
    public final <V extends Expression.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final <V extends Filter.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
