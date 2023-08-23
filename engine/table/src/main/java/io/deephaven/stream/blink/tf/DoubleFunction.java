package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;

import java.util.function.Function;
import java.util.function.ToDoubleFunction;

/**
 * A {@code double} function.
 *
 * @param <T> the input type
 */
@FunctionalInterface
public interface DoubleFunction<T> extends PrimitiveFunction<T>, ToDoubleFunction<T> {

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
     * Creates the function composition {@code g ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsDouble(f.apply(x))}.
     *
     * @param f the inner function
     * @param g the outer function
     * @return the double function
     * @param <T> the input type
     * @param <R> the intermediate type
     */
    static <T, R> DoubleFunction<T> map(Function<T, R> f, DoubleFunction<R> g) {
        return new DoubleMap<>(f, g);
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

    @FunctionalInterface
    interface DoubleToObject<R> {
        R apply(double value);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.apply(this.applyAsDouble(x))}.
     *
     * @param g the outer function
     * @return the object function
     */
    default <R> ObjectFunction<T, R> mapObj(DoubleToObject<R> g, GenericType<R> returnType) {
        return new DoubleToObjectMap<>(this, g, returnType);
    }
}
