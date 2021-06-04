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
import io.deephaven.qst.ReturnTypeLogic;
import io.deephaven.qst.table.column.type.ShortType;
import io.deephaven.qst.table.column.type.StringType;
import java.util.Objects;

abstract class BooleanLogicBase implements ReturnTypeLogic<Boolean> {

    @Override
    public final <T> Boolean transform(ColumnType<T> inputType, T inputValue) {
        return inputValue == null ?
            null :
            inputType.walk(new Transform(inputValue)).getOut();
    }

    public abstract boolean transform(byte x);

    public abstract boolean transform(char x);

    public abstract boolean transform(short x);

    public abstract boolean transform(int x);

    public abstract boolean transform(long x);

    public abstract boolean transform(float x);

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
            out = ColumnType.castValue(booleanType, in);
        }

        @Override
        public void visit(ByteType byteType) {
            out = transform(ColumnType.castValue(byteType, in));
        }

        @Override
        public void visit(CharType charType) {
            out = transform(ColumnType.castValue(charType, in));
        }

        @Override
        public void visit(ShortType shortType) {
            out = transform(ColumnType.castValue(shortType, in));
        }

        @Override
        public void visit(IntType intType) {
            out = transform(ColumnType.castValue(intType, in));
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
