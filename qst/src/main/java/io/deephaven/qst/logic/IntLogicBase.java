package io.deephaven.qst.logic;

import io.deephaven.qst.column.type.BooleanType;
import io.deephaven.qst.column.type.ByteType;
import io.deephaven.qst.column.type.CharType;
import io.deephaven.qst.column.type.ColumnType;
import io.deephaven.qst.column.type.ColumnType.Visitor;
import io.deephaven.qst.column.type.DoubleType;
import io.deephaven.qst.column.type.GenericType;
import io.deephaven.qst.column.type.IntType;
import io.deephaven.qst.column.type.LongType;
import io.deephaven.qst.ReturnTypeLogic;
import io.deephaven.qst.column.type.ShortType;
import io.deephaven.qst.column.type.StringType;
import java.util.Objects;

abstract class IntLogicBase implements ReturnTypeLogic<Integer> {

    @Override
    public final <T> Integer transform(ColumnType<T> inputType, T inputValue) {
        return inputValue == null ?
            null :
            inputType.walk(new Transform(inputValue)).getOut();
    }

    public abstract int transform(boolean x);

    public abstract int transform(byte x);

    public abstract int transform(char x);

    public abstract int transform(short x);

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
        public void visit(ByteType byteType) {
            out = transform(byteType.castValue(in));
        }

        @Override
        public void visit(CharType charType) {
            out = transform(charType.castValue(in));
        }

        @Override
        public void visit(ShortType shortType) {
            out = transform(shortType.castValue(in));
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
