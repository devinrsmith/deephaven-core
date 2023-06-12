package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.Type;

public interface TypedFunction<T> {

    Type<?> returnType();

    <V> V walk(Visitor<T, V> visitor);

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
