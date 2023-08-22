package io.deephaven.stream.blink.tf;

import java.util.Collection;
import java.util.List;

class BooleanAnd<T> implements BooleanFunction<T> {
    private final Collection<BooleanFunction<T>> functions;

    public BooleanAnd(Collection<BooleanFunction<T>> functions) {
        this.functions = List.copyOf(functions);
    }

    @Override
    public boolean applyAsBoolean(T value) {
        for (BooleanFunction<T> function : functions) {
            if (!function.applyAsBoolean(value)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BooleanAnd<?> that = (BooleanAnd<?>) o;

        return functions.equals(that.functions);
    }

    @Override
    public int hashCode() {
        return functions.hashCode();
    }
}
