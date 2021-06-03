package io.deephaven.qst;

import org.immutables.value.Value.Immutable;

@Immutable(builder = false, copy = false)
public abstract class StringType extends ColumnTypeBase<String> {

    public static StringType instance() {
        return ImmutableStringType.of();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return StringType.class.getName();
    }
}
