package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.util.function.Function;
import java.util.function.ToIntFunction;

@FunctionalInterface
public interface IntFunction<T> extends PrimitiveFunction<T>, ToIntFunction<T> {

    /**
     * Assumes the object value is directly castable to an int. Equivalent to {@code x -> (int)x}.
     *
     * @return the int function
     * @param <T> the value type
     */
    static <T> IntFunction<T> primitive() {
        //noinspection unchecked
        return (IntFunction<T>) Functions.PrimitiveInt.INSTANCE;
    }

    @Override
    int applyAsInt(T value);

    @Override
    default IntType returnType() {
        return Type.intType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }

    @Override
    default IntFunction<T> mapInput(Function<T, T> f) {
        return x -> applyAsInt(f.apply(x));
    }

    /**
     * Create a new function which returns {@code onNull} when the value is {@code null}, and otherwise calls {@link #applyAsInt(Object)}. Equivalent to {@code x -> x == null ? onNull : applyAsInt(x)}.
     * @param onNull the value to return on null
     * @return the new int function
     */
    default IntFunction<T> onNullInput(int onNull) {
        return x -> x == null ? onNull : applyAsInt(x);
    }
}
