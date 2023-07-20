package io.deephaven.stream.blink.tf;

public interface PrimitiveFunction<T> extends TypedFunction<T> {

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
