package io.deephaven.qst;

import java.util.Arrays;
import org.immutables.value.Value.Immutable;

@Immutable(builder = false)
public abstract class BooleanType extends ColumnTypeBase<Boolean> {

    public static BooleanType instance() {
        return ImmutableBooleanType.of();
    }

    BooleanType() {
        super(Arrays.asList(boolean.class, Boolean.class));
    }

    @Override
    public final boolean isValidValue(Boolean item) {
        return true;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return BooleanType.class.getName();
    }
}
