package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.Type;

import java.util.function.Function;

public interface TypedFunction<T> {

    Type<?> returnType();

    <V> V walk(Visitor<T, V> visitor);

    /**
     * Creates a new function whose value is first transformed into the same type by {@code f} before application. The
     * semantics are equivalent to {@code x -> theApplyFunction(f.apply(x))}.
     *
     * @param f the input function
     * @return the new function
     */
    TypedFunction<T> mapInput(Function<T, T> f);

    interface Visitor<T, R> {
        R visit(BooleanFunction<T> f);

        R visit(CharFunction<T> f);

        R visit(ByteFunction<T> f);

        R visit(ShortFunction<T> f);

        R visit(IntFunction<T> f);

        R visit(LongFunction<T> f);

        R visit(FloatFunction<T> f);

        R visit(DoubleFunction<T> f);

        R visit(ObjectFunction<T, ?> f);
    }
}
