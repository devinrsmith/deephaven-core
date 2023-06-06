package io.deephaven.app.f;

import java.time.Instant;
import java.util.function.LongFunction;
import java.util.function.ToLongFunction;

@FunctionalInterface
public interface LongMapp<T> extends Mapp<T>, ToLongFunction<T> {

    @Override
    long applyAsLong(T value);

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }

    default <X> ObjectMapp<T, X> andThen(LongFunction<X> f) {
        return t -> f.apply(applyAsLong(t));
    }

    default ObjectMapp<T, Instant> ofEpochMilli() {
        return andThen(Instant::ofEpochMilli);
    }

    default ObjectMapp<T, Instant> ofEpochSecond() {
        return andThen(Instant::ofEpochSecond);
    }
}
