package io.deephaven.stream.blink.tf;

import java.util.Objects;

class BooleanNot<T> implements BooleanFunction<T> {
    private final BooleanFunction<T> function;

    public BooleanNot(BooleanFunction<T> function) {
        this.function = Objects.requireNonNull(function);
    }

    @Override
    public boolean applyAsBoolean(T value) {
        return !function.applyAsBoolean(value);
    }
}
