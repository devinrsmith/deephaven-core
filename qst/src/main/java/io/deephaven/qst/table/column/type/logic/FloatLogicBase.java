package io.deephaven.qst.table.column.type.logic;

import io.deephaven.qst.table.column.type.BooleanType;
import io.deephaven.qst.table.column.type.ByteType;
import io.deephaven.qst.table.column.type.CharType;
import io.deephaven.qst.table.column.type.ColumnType;
import io.deephaven.qst.table.column.type.ColumnType.Visitor;
import io.deephaven.qst.table.column.type.DoubleType;
import io.deephaven.qst.table.column.type.GenericType;
import io.deephaven.qst.table.column.type.IntType;
import io.deephaven.qst.table.column.type.LongType;
import io.deephaven.qst.table.column.type.ShortType;
import io.deephaven.qst.table.column.type.StringType;
import java.util.Objects;

abstract class FloatLogicBase implements ReturnTypeLogic<Float> {

    @Override
    public final <T> Float transform(ColumnType<T> inputType, T inputValue) {
        return inputValue == null ?
            null :
            inputType.walk(new Transform(inputValue)).getOut();
    }

    public abstract float transform(boolean x);

    public abstract float transform(byte x);

    public abstract float transform(char x);

    public abstract float transform(short x);

    public abstract float transform(int x);

    public abstract float transform(long x);

    public abstract float transform(double x);

    public abstract float transform(String x);

    public abstract <T> float transform(GenericType<T> type, T value);

    class Transform implements Visitor {

        private final Object in;
        private Float out;

        public Transform(Object in) {
            this.in = Objects.requireNonNull(in);
        }

        public Float getOut() {
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
            out = transform(longType.castValue(in));
        }

        @Override
        public void visit(StringType stringType) {
            out = transform(stringType.castValue(in));
        }

        @Override
        public void visit(FloatType floatType) {
            out = floatType.castValue(in);
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
