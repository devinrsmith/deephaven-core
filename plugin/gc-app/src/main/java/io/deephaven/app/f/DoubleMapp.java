package io.deephaven.app.f;

import java.util.function.ToDoubleFunction;

@FunctionalInterface
public interface DoubleMapp<T> extends Mapp<T>, ToDoubleFunction<T> {

    @Override
    double applyAsDouble(T value);

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }
}
