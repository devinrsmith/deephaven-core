package io.deephaven.stream.blink;

import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.Type;

@FunctionalInterface
public interface FloatMapp<T> extends Mapp<T> {

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
