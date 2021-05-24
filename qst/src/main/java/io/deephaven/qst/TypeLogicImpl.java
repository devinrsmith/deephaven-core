package io.deephaven.qst;

import io.deephaven.qst.ColumnType.Visitor;
import java.util.Objects;
import org.immutables.value.Value.Immutable;

@Immutable
abstract class TypeLogicImpl implements TypeLogic {

    public static TypeLogicImpl strict() {
        return null;
    }

    public static TypeLogicImpl lax() {
        return null;
    }

    public abstract ReturnTypeLogic<Integer> intLogic();

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
        public void visit(IntType intType) {
            //noinspection unchecked
            out = (ReturnTypeLogic<R>) intLogic();
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

    enum IntWhat implements ReturnTypeLogic<Integer> {
        INSTANCE;


        @Override
        public final <T> Integer transform(ColumnType<T> inputType, T inputValue) {
            return null;
        }
    }


}
