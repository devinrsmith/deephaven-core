package io.deephaven.qst;

import java.util.Arrays;
import java.util.Objects;

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

    @Override
    public final String toString() {
        return IntType.class.getName();
    }

    @Override
    public final <R> Integer transformValue(ColumnType<R> otherType, R otherValue, boolean allowNarrowing) {
        if (otherValue == null) {
            return null;
        }

        return allowNarrowing ? null : otherType.walk(new ToStrict<>(otherValue)).getOut();
    }

    static class ToStrict<R> implements Visitor {
        private final R inValue;

        private Integer out;

        public ToStrict(R inValue) {
            this.inValue = Objects.requireNonNull(inValue);
        }

        public Integer getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(IntType intType) {
            out = intType.castValue(inValue);
        }

        @Override
        public void visit(StringType stringType) {
            out = Integer.parseInt(stringType.castValue(inValue));
        }

        @Override
        public void visit(DoubleType doubleType) {
            throw new IllegalArgumentException("Unable to perform narrowing conversions from double to int");
        }

        @Override
        public void visit(GenericType<?> genericType) {
            throw new IllegalArgumentException("Unable to perform conversions from generic type to int");
        }
    }
}
