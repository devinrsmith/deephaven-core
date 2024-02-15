/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.function;

import io.deephaven.qst.type.Type;

import java.util.function.Function;

/**
 * A common function interface that allows for differentiation based on the return type.
 *
 * @param <T> the input type
 * @see ToPrimitiveFunction
 * @see ToObjectFunction
 */
public interface TypedFunction<T> {

    static <T, R> TypedFunction<T> map(Function<? super T, ? extends R> f, TypedFunction<? super R> g) {
        return TypedFunctions.map2(f, g);
    }

    /**
     * This function's return type.
     *
     * @return the type
     */
    Type<?> returnType();

    <R> R walk(Visitor<T, R> visitor);

    interface Visitor<T, R> {
        R visit(ToPrimitiveFunction<T> f);

        R visit(ToObjectFunction<T, ?> f);
    }
}
