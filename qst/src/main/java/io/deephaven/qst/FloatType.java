package io.deephaven.qst;

import org.immutables.value.Value.Immutable;

@Immutable(builder = false)
public abstract class FloatType extends ColumnTypeBase<Float> {

    public static FloatType instance() {
        return ImmutableFloatType.of();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return FloatType.class.getName();
    }
}
