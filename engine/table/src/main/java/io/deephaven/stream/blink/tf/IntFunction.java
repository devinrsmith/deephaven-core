package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.Type;

import java.util.function.ToIntFunction;

@FunctionalInterface
public interface IntFunction<T> extends TypedFunction<T>, ToIntFunction<T> {

    @Override
    int applyAsInt(T value);

    @Override
    default IntType returnType() {
        return Type.intType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }
}
