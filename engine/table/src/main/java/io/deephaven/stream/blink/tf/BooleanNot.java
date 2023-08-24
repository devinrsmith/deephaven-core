package io.deephaven.stream.blink.tf;

import java.util.Objects;

class BooleanNot<T> implements BooleanFunction<T> {
    private final BooleanFunction<T> function;

    public BooleanNot(BooleanFunction<T> function) {
        this.function = Objects.requireNonNull(function);
    }

    @Override
    public boolean test(T value) {
        return !function.test(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BooleanNot<?> that = (BooleanNot<?>) o;

        return function.equals(that.function);
    }

    @Override
    public int hashCode() {
        return function.hashCode();
    }
}
