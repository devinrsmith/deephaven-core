package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.Type;

public interface TypedFunction<T> {

//    static <X> IntMapp<X> of(IntMapp<X> x) {
//        return x;
//    }
//
//    static <X> LongMapp<X> of(LongMapp<X> x) {
//        return x;
//    }
//
//    static <X, R> ObjectMapp<X, R> of(ObjectMapp<X, R> x) {
//        return x;
//    }
//
//    static <X> ObjectMapp<X, Instant> ofEpochMilli(LongMapp<X> longMapp) {
//        return longMapp.andThen(Instant::ofEpochMilli, null);
//    }
//
//    static <X> ObjectMapp<X, Instant> ofEpochSecond(LongMapp<X> longMapp) {
//        return longMapp.andThen(Instant::ofEpochSecond, null);
//    }

    Type<?> returnType();

    <V> V walk(Visitor<T, V> visitor);

    interface Visitor<T, R> {
        R visit(BooleanFunction<T> booleanMapp);

        R visit(CharFunction<T> charMapp);

        R visit(ByteFunction<T> byteMapp);

        R visit(ShortFunction<T> shortMapp);

        R visit(IntFunction<T> intMapp);

        R visit(LongFunction<T> longMapp);

        R visit(FloatFunction<T> floatMapp);

        R visit(DoubleFunction<T> doubleMapp);

        R visit(ObjectFunction<T, ?> objectMapp);
    }
}
