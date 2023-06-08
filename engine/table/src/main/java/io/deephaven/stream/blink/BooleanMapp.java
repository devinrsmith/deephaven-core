package io.deephaven.stream.blink;

import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.Type;

@FunctionalInterface
public interface BooleanMapp<T> extends Mapp<T> {

    boolean applyAsBoolean(T value);

    @Override
    default BooleanType returnType() {
        return Type.booleanType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }
}
