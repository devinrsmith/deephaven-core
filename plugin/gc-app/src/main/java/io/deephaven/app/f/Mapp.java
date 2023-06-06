package io.deephaven.app.f;

import java.time.Instant;

public interface Mapp<T>  {

    static <X> IntMapp<X> of(IntMapp<X> x) {
        return x;
    }

    static <X> LongMapp<X> of(LongMapp<X> x) {
        return x;
    }

    static <X, R> ObjectMapp<X, R> of(ObjectMapp<X, R> x) {
        return x;
    }

    static <X> ObjectMapp<X, Instant> ofEpochMilli(LongMapp<X> longMapp) {
        return longMapp.andThen(Instant::ofEpochMilli);
    }

    static <X> ObjectMapp<X, Instant> ofEpochSecond(LongMapp<X> longMapp) {
        return longMapp.andThen(Instant::ofEpochSecond);
    }

    <V> V walk(Visitor<T, V> visitor);

    interface Visitor<T, R> {
        R visit(CharMapp<T> charMapp);

        R visit(ByteMapp<T> byteMapp);

        R visit(ShortMapp<T> intMapp);

        R visit(IntMapp<T> intMapp);

        R visit(LongMapp<T> longMapp);

        R visit(FloatMapp<T> floatMapp);

        R visit(DoubleMapp<T> doubleMapp);

        R visit(ObjectMapp<T, ?> objectMapp);
    }
}
