package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;

import java.util.function.Function;

@FunctionalInterface
public interface FloatFunction<T> extends PrimitiveFunction<T> {
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

    static <T> FloatFunction<T> cast(TypedFunction<T> f) {
        return (FloatFunction<T>) f;
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

    @FunctionalInterface
    interface FloatToObject<R> {
        R apply(float value);
    }

    default <R> ObjectFunction<T, R> mapObj(FloatToObject<R> f, GenericType<R> returnType) {
        return ObjectFunction.of(t -> f.apply(applyAsFloat(t)), returnType);
    }
}
