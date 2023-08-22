package io.deephaven.stream.blink.tf;

import java.util.Objects;
import java.util.function.Function;

class DoubleMap<T, R> implements DoubleFunction<T> {
    private final Function<T, R> f;
    private final DoubleFunction<R> g;

    public DoubleMap(Function<T, R> f, DoubleFunction<R> g) {
        this.f = Objects.requireNonNull(f);
        this.g = Objects.requireNonNull(g);
    }

    @Override
    public double applyAsDouble(T value) {
        return g.applyAsDouble(f.apply(value));
    }
}
