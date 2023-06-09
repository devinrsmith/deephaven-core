package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.Type;

import java.time.Instant;
import java.util.function.ToLongFunction;

@FunctionalInterface
public interface LongFunction<T> extends TypedFunction<T>, ToLongFunction<T> {

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

    default <X> ObjectFunction<T, X> andThen(java.util.function.LongFunction<X> f, GenericType<X> returnType) {
        return ObjectFunction.of(t -> f.apply(applyAsLong(t)), returnType);
    }

    default ObjectFunction<T, Instant> ofEpochMilli() {
        return andThen(Instant::ofEpochMilli, Type.instantType());
    }

    default ObjectFunction<T, Instant> ofEpochSecond() {
        return andThen(Instant::ofEpochSecond, Type.instantType());
    }
}
