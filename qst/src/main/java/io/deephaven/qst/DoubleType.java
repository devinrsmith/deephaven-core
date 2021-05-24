package io.deephaven.qst;

import java.util.Arrays;
import java.util.Objects;

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

    @Override
    public final String toString() {
        return DoubleType.class.getName();
    }

    @Override
    public final <R> Double transformValue(ColumnType<R> otherType, R otherValue) {
        return otherValue == null ? null : otherType.walk(new ToDouble(otherValue)).getOut();
    }

    static class ToDouble implements Visitor {

        private final Object inValue;
        private Double out;

        public ToDouble(Object inValue) {
            this.inValue = Objects.requireNonNull(inValue);
        }

        public Double getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(IntType intType) {
            out = intType.strictCast(inValue).doubleValue();
        }

        @Override
        public void visit(StringType stringType) {
            out = Double.parseDouble(stringType.strictCast(inValue));
        }

        @Override
        public void visit(DoubleType doubleType) {
            out = doubleType.strictCast(inValue);
        }

        @Override
        public void visit(GenericType<?> genericType) {
            throw new RuntimeException("todo");
        }
    }
}
