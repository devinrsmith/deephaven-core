package io.deephaven.qst;

import io.deephaven.qst.ColumnType.Visitor;
import java.util.Objects;

abstract class DoubleLogicBase implements ReturnTypeLogic<Double> {

    @Override
    public final <T> Double transform(ColumnType<T> inputType, T inputValue) {
        return inputValue == null ?
            null :
            inputType.walk(new Transform(inputValue)).getOut();
    }

    public abstract double transform(int x);

    public abstract double transform(String x);

    public abstract <T> double transform(GenericType<T> type, T value);

    class Transform implements Visitor {

        private final Object in;
        private Double out;

        public Transform(Object in) {
            this.in = Objects.requireNonNull(in);
        }

        public Double getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(IntType intType) {
            out = transform(intType.castValue(in));
        }

        @Override
        public void visit(StringType stringType) {
            out = transform(stringType.castValue(in));
        }

        @Override
        public void visit(DoubleType doubleType) {
            out = doubleType.castValue(in);
        }

        @Override
        public void visit(GenericType<?> genericType) {
            //noinspection unchecked
            out = transform((GenericType)genericType, in);
        }
    }
}
