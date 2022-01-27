package io.deephaven.plugin.app;

import java.util.Objects;
import java.util.function.Supplier;

public final class LazyState implements State {

    private final Supplier<State> supplier;

    public LazyState(Supplier<State> supplier) {
        this.supplier = Objects.requireNonNull(supplier);
    }

    @Override
    public void insertInto(Consumer consumer) {
        supplier.get().insertInto(consumer);
    }

    @Override
    public String toString() {
        return "LazyState(" + supplier + ")";
    }
}
