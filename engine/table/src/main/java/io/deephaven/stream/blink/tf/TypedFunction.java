package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.Type;

public interface TypedFunction<T> {

    static <X> IntFunction<X> of(IntFunction<X> x) {
        return x;
    }

    static <X> LongFunction<X> of(LongFunction<X> x) {
        return x;
    }
    //
    // static <X, R> ObjectMapp<X, R> of(ObjectMapp<X, R> x) {
    // return x;
    // }
    //
    // static <X> ObjectMapp<X, Instant> ofEpochMilli(LongMapp<X> longMapp) {
    // return longMapp.andThen(Instant::ofEpochMilli, null);
    // }
    //
    // static <X> ObjectMapp<X, Instant> ofEpochSecond(LongMapp<X> longMapp) {
    // return longMapp.andThen(Instant::ofEpochSecond, null);
    // }

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
