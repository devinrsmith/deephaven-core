package io.deephaven.stream.blink;

import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.Type;

import java.util.function.ToDoubleFunction;

@FunctionalInterface
public interface DoubleMapp<T> extends Mapp<T>, ToDoubleFunction<T> {

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
