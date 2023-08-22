package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;

import java.util.function.Function;

/**
 * A {@code float} function.
 *
 * @param <T> the input type
 */
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

    /**
     * Creates the function composition {@code g ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsFloat(f.apply(x))}.
     *
     * @param f the inner function
     * @param g the outer function
     * @return the float function
     * @param <T> the input type
     * @param <R> the intermediate type
     */
    static <T, R> FloatFunction<T> map(Function<T, R> f, FloatFunction<R> g) {
        return new FloatMap<>(f, g);
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

    @FunctionalInterface
    interface FloatToObject<R> {
        R apply(float value);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.apply(this.applyAsFloat(x))}.
     *
     * @param g the outer function
     * @return the object function
     */
    default <R> ObjectFunction<T, R> mapObj(FloatToObject<R> g, GenericType<R> returnType) {
        return new FloatToObjectMap<>(this, g, returnType);
    }
}
