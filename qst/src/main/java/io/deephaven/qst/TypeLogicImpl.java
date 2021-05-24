package io.deephaven.qst;

import io.deephaven.qst.ColumnType.Visitor;
import java.util.Objects;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class TypeLogicImpl implements TypeLogic {

    public static TypeLogicImpl strict() {
        return ImmutableTypeLogicImpl.builder()
            .booleanLogic(BooleanLogicStrict.instance())
            .intLogic(IntLogicStrict.instance())
            .longLogic(LongLogicStrict.instance())
            .doubleLogic(DoubleLogic.instance())
            .stringLogic(StringLogic.instance())
            .build();
    }

    public static TypeLogicImpl lax() {
        return ImmutableTypeLogicImpl.builder()
            .booleanLogic(BooleanLogicLax.instance())
            .intLogic(IntLogicLax.instance())
            .longLogic(LongLogicLax.instance())
            .doubleLogic(DoubleLogic.instance())
            .stringLogic(StringLogic.instance())
            .build();
    }

    public abstract ReturnTypeLogic<Boolean> booleanLogic();

    public abstract ReturnTypeLogic<Integer> intLogic();

    public abstract ReturnTypeLogic<Long> longLogic();

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
