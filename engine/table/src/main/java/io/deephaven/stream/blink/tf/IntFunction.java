package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.Type;

import java.util.function.Function;
import java.util.function.ToIntFunction;

/**
 * An {@code int} function.
 *
 * @param <T> the input type
 */
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

    static <T> IntFunction<T> cast(TypedFunction<T> f) {
        return (IntFunction<T>) f;
    }

    /**
     * Creates the function composition {@code g ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsInt(f.apply(x))}.
     *
     * @param f the inner function
     * @param g the outer function
     * @return the int function
     * @param <T> the input type
     * @param <R> the intermediate type
     */
    static <T, R> IntFunction<T> map(Function<T, R> f, IntFunction<R> g) {
        return new IntMap<>(f, g);
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

    @FunctionalInterface
    interface IntToObject<R> {
        R apply(int value);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.apply(this.applyAsInt(x))}.
     *
     * @param g the outer function
     * @return the object function
     */
    default <R> ObjectFunction<T, R> mapObj(IntToObject<R> g, GenericType<R> returnType) {
        return new IntToObjectMap<>(this, g, returnType);
    }
}
