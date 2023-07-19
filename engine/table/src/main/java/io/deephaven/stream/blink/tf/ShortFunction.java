package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.util.function.Function;

@FunctionalInterface
public interface ShortFunction<T> extends PrimitiveFunction<T> {

    /**
     * Assumes the object value is directly castable to a short. Equivalent to {@code x -> (short)x}.
     *
     * @return the short function
     * @param <T> the value type
     */
    static <T> ShortFunction<T> primitive() {
        //noinspection unchecked
        return (ShortFunction<T>) Functions.PrimitiveShort.INSTANCE;
    }

    /**
     * Assumes the object value is {@code null} or directly castable to a short. On null, the function returns {@link QueryConstants#NULL_SHORT}. Equivalent to {@code NullGuard.of(primitive())}.
     *
     * @return the guarded short function
     * @param <T> the value type
     * @see #primitive()
     * @see NullGuard#of(ShortFunction)
     */
    static <T> ShortFunction<T> guardedPrimitive() {
        //noinspection unchecked
        return (ShortFunction<T>) Functions.SHORT_GUARDED;
    }

    short applyAsShort(T value);

    @Override
    default ShortType returnType() {
        return Type.shortType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }

    @Override
    default ShortFunction<T> mapInput(Function<T, T> f) {
        return x -> applyAsShort(f.apply(x));
    }

    /**
     * Create a new function which returns {@code onNull} when the value is {@code null}, and otherwise calls {@link #applyAsShort(Object)}. Equivalent to {@code x -> x == null ? onNull : applyAsShort(x)}.
     * @param onNull the value to return on null
     * @return the new short function
     */
    default ShortFunction<T> onNullInput(short onNull) {
        return x -> x == null ? onNull : applyAsShort(x);
    }
}
