package io.deephaven.stream.blink.tf;

import java.util.Collection;
import java.util.List;

class BooleanOr<T> implements BooleanFunction<T> {
    private final Collection<BooleanFunction<T>> functions;

    public BooleanOr(Collection<BooleanFunction<T>> functions) {
        this.functions = List.copyOf(functions);
    }

    @Override
    public boolean test(T value) {
        for (BooleanFunction<T> function : functions) {
            if (function.test(value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BooleanOr<?> booleanOr = (BooleanOr<?>) o;

        return functions.equals(booleanOr.functions);
    }

    @Override
    public int hashCode() {
        return functions.hashCode();
    }
}
