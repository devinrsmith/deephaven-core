package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.Type;

import java.util.function.Function;

@FunctionalInterface
public interface FloatFunction<T> extends TypedFunction<T> {

    float applyAsFloat(T value);

    @Override
    default FloatType returnType() {
        return Type.floatType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }

    @Override
    default FloatFunction<T> mapInput(Function<T, T> f) {
        return x -> applyAsFloat(f.apply(x));
    }

    default FloatFunction<T> onNull(float onNull) {
        return x -> x == null ? onNull : applyAsFloat(x);
    }
}
