package io.deephaven.qst;

import org.immutables.value.Value.Immutable;

@Immutable(builder = false)
public abstract class DoubleType extends ColumnTypeBase<Double> {

    public static DoubleType instance() {
        return ImmutableDoubleType.of();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return DoubleType.class.getName();
    }
}
