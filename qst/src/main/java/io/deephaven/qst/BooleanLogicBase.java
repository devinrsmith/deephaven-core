package io.deephaven.qst;

import io.deephaven.qst.ColumnType.Visitor;
import java.util.Objects;

abstract class BooleanLogicBase implements ReturnTypeLogic<Boolean> {

    @Override
    public final <T> Boolean transform(ColumnType<T> inputType, T inputValue) {
        return inputValue == null ?
            null :
            inputType.walk(new Transform(inputValue)).getOut();
    }

    public abstract boolean transform(int x);

    public abstract boolean transform(long x);

    public abstract boolean transform(double x);

    public abstract boolean transform(String x);

    public abstract <T> boolean transform(GenericType<T> type, T value);

    class Transform implements Visitor {

        private final Object in;
        private Boolean out;

        public Transform(Object in) {
            this.in = Objects.requireNonNull(in);
        }

        public Boolean getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(BooleanType booleanType) {
            out = booleanType.castValue(in);
        }

        @Override
        public void visit(IntType intType) {
            out = transform(intType.castValue(in));
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
