package io.deephaven.qst.logic;

import io.deephaven.qst.column.BooleanType;
import io.deephaven.qst.column.ByteType;
import io.deephaven.qst.column.CharType;
import io.deephaven.qst.column.ColumnType;
import io.deephaven.qst.column.ColumnType.Visitor;
import io.deephaven.qst.column.DoubleType;
import io.deephaven.qst.column.GenericType;
import io.deephaven.qst.column.IntType;
import io.deephaven.qst.column.LongType;
import io.deephaven.qst.ReturnTypeLogic;
import io.deephaven.qst.column.ShortType;
import io.deephaven.qst.column.StringType;
import java.util.Objects;

abstract class LongLogicBase implements ReturnTypeLogic<Long> {

    @Override
    public final <T> Long transform(ColumnType<T> inputType, T inputValue) {
        return inputValue == null ?
            null :
            inputType.walk(new Transform(inputValue)).getOut();
    }

    public abstract long transform(boolean x);

    public abstract long transform(byte x);

    public abstract long transform(char x);

    public abstract long transform(short x);

    public abstract long transform(int x);

    public abstract long transform(float x);

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
