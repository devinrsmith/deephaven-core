package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.Type;

import java.util.function.ToDoubleFunction;

@FunctionalInterface
public interface DoubleFunction<T> extends TypedFunction<T>, ToDoubleFunction<T> {

    @Override
    double applyAsDouble(T value);

    @Override
    default DoubleType returnType() {
        return Type.doubleType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }
}
