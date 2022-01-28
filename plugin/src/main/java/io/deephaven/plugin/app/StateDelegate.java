package io.deephaven.plugin.app;

import java.util.Objects;

public class StateDelegate implements State {
    private final State impl;

    public StateDelegate(State impl) {
        this.impl = Objects.requireNonNull(impl);
    }

    @Override
    public final void insertInto(Consumer consumer) {
        impl.insertInto(consumer);
    }
}
