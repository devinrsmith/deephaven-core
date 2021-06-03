package io.deephaven.qst;

import io.deephaven.qst.ColumnType.Visitor;
import java.util.Objects;

abstract class IntLogicBase implements ReturnTypeLogic<Integer> {

    @Override
    public final <T> Integer transform(ColumnType<T> inputType, T inputValue) {
        return inputValue == null ?
            null :
            inputType.walk(new Transform(inputValue)).getOut();
    }

    public abstract int transform(boolean x);

    public abstract int transform(long x);

    public abstract int transform(float x);

    public abstract int transform(double x);

    public abstract int transform(String x);

    public abstract <T> int transform(GenericType<T> type, T value);

    class Transform implements Visitor {

        private final Object in;
        private Integer out;

        public Transform(Object in) {
            this.in = Objects.requireNonNull(in);
        }

        public Integer getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(BooleanType booleanType) {
            out = transform(booleanType.castValue(in));
        }

        @Override
        public void visit(IntType intType) {
            out = intType.castValue(in);
        }

        @Override
        public void visit(LongType longType) {
            out = transform(longType.castValue(in));
        }

        @Override
        public void visit(StringType stringType) {
            out = transform(stringType.castValue(in));
        }

        @Override
        public void visit(FloatType floatType) {
            out = transform(floatType.castValue(in));
        }

        @Override
        public void visit(DoubleType doubleType) {
            out = transform(doubleType.castValue(in));
        }

        @Override
        public void visit(GenericType<?> genericType) {
            //noinspection unchecked
            out = transform((GenericType)genericType, in);
        }
    }
}
