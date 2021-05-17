package io.deephaven.datastructures.util;

import java.util.Arrays;

public final class DoubleType extends ColumnTypeBase<Double> {

    private static final DoubleType INSTANCE = new DoubleType();

    public static DoubleType instance() {
        return INSTANCE;
    }

    private DoubleType() {
        super(Arrays.asList(double.class, Double.class));
    }

    @Override
    public boolean isValidValue(Double item) {
        return item == null || item != -Double.MAX_VALUE; // todo QueryConstants
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
