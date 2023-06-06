package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.util.function.Function;
import java.util.function.ToDoubleFunction;

@FunctionalInterface
public interface DoubleFunction<T> extends TypedFunction<T>, ToDoubleFunction<T> {

    /**
     * Assumes the object value is directly castable to a double. Equivalent to {@code x -> (double)x}.
     *
     * @return the double function
     * @param <T> the value type
     */
    static <T> DoubleFunction<T> primitive() {
        //noinspection unchecked
        return (DoubleFunction<T>) Functions.PrimitiveDouble.INSTANCE;
    }

    /**
     * Assumes the object value is {@code null} or directly castable to a double. On null, the function returns {@link QueryConstants#NULL_DOUBLE}. Equivalent to {@code NullGuard.of(primitive())}.
     *
     * @return the guarded double function
     * @param <T> the value type
     * @see #primitive()
     * @see NullGuard#of(DoubleFunction)
     */
    static <T> DoubleFunction<T> guardedPrimitive() {
        //noinspection unchecked
        return (DoubleFunction<T>) Functions.DOUBLE_GUARDED;
    }

    @Override
    double applyAsDouble(T value);

    @Override
    default DoubleType returnType() {
        return Type.doubleType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }

    @Override
    default DoubleFunction<T> mapInput(Function<T, T> f) {
        return x -> applyAsDouble(f.apply(x));
    }

    /**
     * Create a new function which returns {@code onNull} when the value is {@code null}, and otherwise calls {@link #applyAsDouble(Object)}. Equivalent to {@code x -> x == null ? onNull : applyAsDouble(x)}.
     * @param onNull the value to return on null
     * @return the new double function
     */
    default DoubleFunction<T> onNullInput(double onNull) {
        return x -> x == null ? onNull : applyAsDouble(x);
    }
}
