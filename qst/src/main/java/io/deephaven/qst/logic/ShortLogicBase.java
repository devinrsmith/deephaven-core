package io.deephaven.qst.logic;

import io.deephaven.qst.BooleanType;
import io.deephaven.qst.ByteType;
import io.deephaven.qst.CharType;
import io.deephaven.qst.ColumnType;
import io.deephaven.qst.ColumnType.Visitor;
import io.deephaven.qst.DoubleType;
import io.deephaven.qst.GenericType;
import io.deephaven.qst.IntType;
import io.deephaven.qst.LongType;
import io.deephaven.qst.ReturnTypeLogic;
import io.deephaven.qst.ShortType;
import io.deephaven.qst.StringType;
import java.util.Objects;

abstract class ShortLogicBase implements ReturnTypeLogic<Short> {

    @Override
    public final <T> Short transform(ColumnType<T> inputType, T inputValue) {
        return inputValue == null ?
            null :
            inputType.walk(new Transform(inputValue)).getOut();
    }

    public abstract short transform(boolean x);

    public abstract short transform(byte x);

    public abstract short transform(char x);

    public abstract short transform(int x);

    public abstract short transform(long x);

    public abstract short transform(float x);

    public abstract short transform(double x);

    public abstract short transform(String x);

    public abstract <T> short transform(GenericType<T> type, T value);

    class Transform implements Visitor {

        private final Object in;
        private Short out;

        public Transform(Object in) {
            this.in = Objects.requireNonNull(in);
        }

        public Short getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(BooleanType booleanType) {
            out = transform(booleanType.castValue(in));
        }

        @Override
        public void visit(ByteType byteType) {
            out = transform(byteType.castValue(in));
        }

        @Override
        public void visit(CharType charType) {
            out = transform(charType.castValue(in));
        }

        @Override
        public void visit(ShortType shortType) {
            out = shortType.castValue(in);
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
