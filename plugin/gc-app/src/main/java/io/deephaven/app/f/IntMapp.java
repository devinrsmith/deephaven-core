package io.deephaven.app.f;

import java.util.function.ToIntFunction;

@FunctionalInterface
public interface IntMapp<T> extends Mapp<T>, ToIntFunction<T> {

    @Override
    int applyAsInt(T value);

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }
}
