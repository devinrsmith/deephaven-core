/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.Type;

import java.util.function.Function;
import java.util.function.ToLongFunction;

/**
 * A {@code long} function.
 *
 * @param <T> the input type
 */
@FunctionalInterface
public interface LongFunction<T> extends PrimitiveFunction<T>, ToLongFunction<T> {
    /**
     * Assumes the object value is directly castable to a long. Equivalent to {@code x -> (long)x}.
     *
     * @return the long function
     * @param <T> the value type
     */
    static <T> LongFunction<T> primitive() {
        return LongFunctions.primitive();
    }

    /**
     * Creates the function composition {@code g ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsLong(f.apply(x))}.
     *
     * @param f the inner function
     * @param g the outer function
     * @return the long function
     * @param <T> the input type
     * @param <R> the intermediate type
     */
    static <T, R> LongFunction<T> map(Function<T, R> f, LongFunction<R> g) {
        return LongFunctions.map(f, g);
    }

    @Override
    long applyAsLong(T value);

    @Override
    default LongType returnType() {
        return Type.longType();
    }

    @Override
    default LongFunction<T> mapInput(Function<T, T> f) {
        return map(f, this);
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }
}
