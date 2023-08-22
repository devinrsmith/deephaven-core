package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.GenericType;
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
        //noinspection unchecked
        return (ShortFunction<T>) Functions.PrimitiveShort.INSTANCE;
    }

    static <T> ShortFunction<T> cast(TypedFunction<T> f) {
        return (ShortFunction<T>) f;
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
        return new ShortMap<>(f, g);
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

    @FunctionalInterface
    interface ShortToObject<R> {
        R apply(short value);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.apply(this.applyAsShort(x))}.
     *
     * @param g the outer function
     * @return the object function
     */
    default <R> ObjectFunction<T, R> mapObj(ShortToObject<R> g, GenericType<R> returnType) {
        return new ShortToObjectMap<>(this, g, returnType);
    }
}
