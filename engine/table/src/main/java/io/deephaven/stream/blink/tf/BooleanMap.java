package io.deephaven.stream.blink.tf;

import java.util.Objects;
import java.util.function.Function;

class BooleanMap<T, R> implements BooleanFunction<T> {
    private final Function<T, R> f1;
    private final BooleanFunction<R> f2;

    public BooleanMap(Function<T, R> f1, BooleanFunction<R> f2) {
        this.f1 = Objects.requireNonNull(f1);
        this.f2 = Objects.requireNonNull(f2);
    }

    @Override
    public boolean applyAsBoolean(T value) {
        return f2.applyAsBoolean(f1.apply(value));
    }
}
