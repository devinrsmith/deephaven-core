package io.deephaven.stream.blink;

import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.Type;

import java.time.Instant;
import java.util.function.LongFunction;
import java.util.function.ToLongFunction;

@FunctionalInterface
public interface LongMapp<T> extends Mapp<T>, ToLongFunction<T> {

    @Override
    long applyAsLong(T value);

    @Override
    default LongType returnType() {
        return Type.longType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }

    default <X> ObjectMapp<T, X> andThen(LongFunction<X> f, Type<X> returnType) {
        return ObjectMapp.of(t -> f.apply(applyAsLong(t)), returnType);
    }

    default ObjectMapp<T, Instant> ofEpochMilli() {
        return andThen(Instant::ofEpochMilli, Type.instantType());
    }

    default ObjectMapp<T, Instant> ofEpochSecond() {
        return andThen(Instant::ofEpochSecond, Type.instantType());
    }
}
