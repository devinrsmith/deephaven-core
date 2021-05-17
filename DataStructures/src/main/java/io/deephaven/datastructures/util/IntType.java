package io.deephaven.datastructures.util;

import java.util.Arrays;

public final class IntType extends ColumnTypeBase<Integer> {

    private static final IntType INSTANCE = new IntType();

    public static IntType instance() {
        return INSTANCE;
    }

    private IntType() {
        super(Arrays.asList(int.class, Integer.class));
    }

    @Override
    public final boolean isValidValue(Integer item) {
        return item == null || item != Integer.MIN_VALUE; // todo QueryConstants
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
