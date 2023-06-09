package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.Type;

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
}
