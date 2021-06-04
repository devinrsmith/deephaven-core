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
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class TypeLogicImpl implements TypeLogic {

    public static TypeLogicImpl strict() {
        return ImmutableTypeLogicImpl.builder()
            .booleanLogic(BooleanLogicStrict.instance())
            .byteLogic(ByteLogicStrict.instance())
            .charLogic(CharLogicStrict.instance())
            .shortLogic(ShortLogicStrict.instance())
            .intLogic(IntLogicStrict.instance())
            .longLogic(LongLogicStrict.instance())
            .floatLogic(FloatLogic.instance())
            .doubleLogic(DoubleLogic.instance())
            .stringLogic(StringLogic.instance())
            .build();
    }

    public static TypeLogicImpl lax() {
        return ImmutableTypeLogicImpl.builder()
            .booleanLogic(BooleanLogicLax.instance())
            .byteLogic(ByteLogicLax.instance())
            .charLogic(CharLogicLax.instance())
            .shortLogic(ShortLogicLax.instance())
            .intLogic(IntLogicLax.instance())
            .longLogic(LongLogicLax.instance())
            .floatLogic(FloatLogic.instance())
            .doubleLogic(DoubleLogic.instance())
            .stringLogic(StringLogic.instance())
            .build();
    }

    public abstract ReturnTypeLogic<Boolean> booleanLogic();

    public abstract ReturnTypeLogic<Byte> byteLogic();

    public abstract ReturnTypeLogic<Character> charLogic();

    public abstract ReturnTypeLogic<Short> shortLogic();

    public abstract ReturnTypeLogic<Integer> intLogic();

    public abstract ReturnTypeLogic<Long> longLogic();

    public abstract ReturnTypeLogic<Float> floatLogic();

    public abstract ReturnTypeLogic<Double> doubleLogic();

    public abstract ReturnTypeLogic<String> stringLogic();

    @Override
    public final <T, R> R transform(ColumnType<R> returnType, ColumnType<T> inputType, T inputValue) {
        return returnType
            .walk(new SwitchReturnTypes<R>())
            .getOut()
            .transform(inputType, inputValue);
    }

    class SwitchReturnTypes<R> implements Visitor {
        private ReturnTypeLogic<R> out;

        public ReturnTypeLogic<R> getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(BooleanType booleanType) {
            //noinspection unchecked
            out = (ReturnTypeLogic<R>) booleanLogic();
        }

        @Override
        public void visit(ByteType byteType) {
            //noinspection unchecked
            out = (ReturnTypeLogic<R>) byteLogic();
        }

        @Override
        public void visit(CharType charType) {
            //noinspection unchecked
            out = (ReturnTypeLogic<R>) charLogic();
        }

        @Override
        public void visit(ShortType shortType) {
            //noinspection unchecked
            out = (ReturnTypeLogic<R>) shortLogic();
        }

        @Override
        public void visit(IntType intType) {
            //noinspection unchecked
            out = (ReturnTypeLogic<R>) intLogic();
        }

        @Override
        public void visit(LongType longType) {
            //noinspection unchecked
            out = (ReturnTypeLogic<R>) longLogic();
        }

        @Override
        public void visit(StringType stringType) {
            //noinspection unchecked
            out = (ReturnTypeLogic<R>) stringLogic();
        }

        @Override
        public void visit(FloatType floatType) {
            //noinspection unchecked
            out = (ReturnTypeLogic<R>) floatLogic();
        }

        @Override
        public void visit(DoubleType doubleType) {
            //noinspection unchecked
            out = (ReturnTypeLogic<R>) doubleLogic();
        }

        @Override
        public void visit(GenericType<?> genericType) {
            throw new IllegalArgumentException("todo");
        }
    }
}
