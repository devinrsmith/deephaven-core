package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.Type;

import java.util.function.Function;

/**
 * A {@code short} function.
 *
 * @param <T> the input type
 */
@FunctionalInterface
public interface ShortFunction<T> extends PrimitiveFunction<T> {

    /**
     * Assumes the object value is directly castable to a short. Equivalent to {@code x -> (short)x}.
     *
     * @return the short function
     * @param <T> the value type
     */
    static <T> ShortFunction<T> primitive() {
        return ShortFunctions.primitive();
    }

    /**
     * Creates the function composition {@code g ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsShort(f.apply(x))}.
     *
     * @param f the inner function
     * @param g the outer function
     * @return the short function
     * @param <T> the input type
     * @param <R> the intermediate type
     */
    static <T, R> ShortFunction<T> map(Function<T, R> f, ShortFunction<R> g) {
        return ShortFunctions.map(f, g);
    }

    /**
     * Applies this function to the given argument.
     *
     * @param value the function argument
     * @return the function result
     */
    short applyAsShort(T value);

    @Override
    default ShortType returnType() {
        return Type.shortType();
    }

    @Override
    default ShortFunction<T> mapInput(Function<T, T> f) {
        return map(f, this);
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }
}
