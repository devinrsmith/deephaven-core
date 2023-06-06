package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.util.function.Function;

@FunctionalInterface
public interface FloatFunction<T> extends TypedFunction<T> {
    /**
     * Assumes the object value is directly castable to a float. Equivalent to {@code x -> (float)x}.
     *
     * @return the float function
     * @param <T> the value type
     */
    static <T> FloatFunction<T> primitive() {
        //noinspection unchecked
        return (FloatFunction<T>) Functions.PrimitiveFloat.INSTANCE;
    }

    /**
     * Assumes the object value is {@code null} or directly castable to a float. On null, the function returns {@link QueryConstants#NULL_FLOAT}. Equivalent to {@code NullGuard.of(primitive()}.
     *
     * @return the guarded float function
     * @param <T> the value type
     * @see #primitive()
     * @see NullGuard#of(FloatFunction)
     */
    static <T> FloatFunction<T> guardedPrimitive() {
        //noinspection unchecked
        return (FloatFunction<T>) Functions.FLOAT_GUARDED;
    }

    float applyAsFloat(T value);

    @Override
    default FloatType returnType() {
        return Type.floatType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }

    @Override
    default FloatFunction<T> mapInput(Function<T, T> f) {
        return x -> applyAsFloat(f.apply(x));
    }

    /**
     * Create a new function which returns {@code onNull} when the value is {@code null}, and otherwise calls {@link #applyAsFloat(Object)}. Equivalent to {@code x -> x == null ? onNull : applyAsFloat(x)}.
     * @param onNull the value to return on null
     * @return the new float function
     */
    default FloatFunction<T> onNullInput(float onNull) {
        return x -> x == null ? onNull : applyAsFloat(x);
    }
}
