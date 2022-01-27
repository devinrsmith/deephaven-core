package io.deephaven.plugin.app;

import java.util.Objects;

public class AppImpl extends AppBase {

    private final State state;

    public AppImpl(Class<?> clazz, State state) {
        this(clazz.getName(), clazz.getSimpleName(), state);
    }

    public AppImpl(String id, String name, State state) {
        super(id, name);
        this.state = Objects.requireNonNull(state);
    }

    public final State state() {
        return state;
    }

    @Override
    public final void insertInto(Consumer consumer) {
        state.insertInto(consumer);
    }

    @Override
    public String toString() {
        return "AppImpl(" + id() + "," + state + ")";
    }
}
