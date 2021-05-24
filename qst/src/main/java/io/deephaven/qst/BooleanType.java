package io.deephaven.qst;

import java.util.Arrays;
import java.util.Objects;

public final class BooleanType extends ColumnTypeBase<Boolean> {

    private static final BooleanType INSTANCE = new BooleanType();

    public static BooleanType instance() {
        return INSTANCE;
    }

    private BooleanType() {
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
