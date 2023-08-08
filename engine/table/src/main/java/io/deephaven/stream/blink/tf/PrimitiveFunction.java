package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.PrimitiveType;

public interface PrimitiveFunction<T> extends TypedFunction<T> {

    @Override
    PrimitiveType<?> returnType();

    <R> R walk(Visitor<T, R> visitor);

    @Override
    default <R> R walk(TypedFunction.Visitor<T, R> visitor) {
        return visitor.visit(this);
    }

    interface Visitor<T, R> {
        R visit(BooleanFunction<T> f);

        R visit(CharFunction<T> f);

        R visit(ByteFunction<T> f);

        R visit(ShortFunction<T> f);

        R visit(IntFunction<T> f);

        R visit(LongFunction<T> f);

        R visit(FloatFunction<T> f);

        R visit(DoubleFunction<T> f);
    }
}
