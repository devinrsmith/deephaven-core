package io.deephaven.qst;

import io.deephaven.qst.ColumnType.Visitor;
import java.util.Objects;

abstract class LongLogicBase implements ReturnTypeLogic<Long> {

    @Override
    public final <T> Long transform(ColumnType<T> inputType, T inputValue) {
        return inputValue == null ?
            null :
            inputType.walk(new Transform(inputValue)).getOut();
    }

    public abstract long transform(boolean x);

    public abstract long transform(int x);

    public abstract long transform(double x);

    public abstract long transform(String x);

    public abstract <T> long transform(GenericType<T> type, T value);

    class Transform implements Visitor {

        private final Object in;
        private Long out;

        public Transform(Object in) {
            this.in = Objects.requireNonNull(in);
        }

        public Long getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(BooleanType booleanType) {
            out = transform(booleanType.castValue(in));
        }

        @Override
        public void visit(IntType intType) {
            out = transform(intType.castValue(in));
        }

        @Override
        public void visit(LongType longType) {
            out = longType.castValue(in);
        }

        @Override
        public void visit(StringType stringType) {
            out = transform(stringType.castValue(in));
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
