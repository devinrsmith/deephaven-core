package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.Type;

import java.util.function.Function;

@FunctionalInterface
public interface BooleanFunction<T> extends PrimitiveFunction<T> {

    /**
     * Assumes the object value is directly castable to a boolean. Equivalent to {@code x -> (boolean)x}.
     *
     * @return the boolean function
     * @param <T> the value type
     */
    static <T> BooleanFunction<T> primitive() {
        //noinspection unchecked
        return (BooleanFunction<T>) Functions.PrimitiveBoolean.INSTANCE;
    }

    boolean applyAsBoolean(T value);

    @Override
    default BooleanType returnType() {
        return Type.booleanType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }

    @Override
    default BooleanFunction<T> mapInput(Function<T, T> f) {
        return x -> applyAsBoolean(f.apply(x));
    }

    default BooleanFunction<T> onNullInput(boolean onNull) {
        return x -> x == null ? onNull : applyAsBoolean(x);
    }
}
