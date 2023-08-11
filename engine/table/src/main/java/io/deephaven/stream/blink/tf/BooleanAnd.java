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
}
