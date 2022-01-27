package io.deephaven.plugin.app;

public final class StateAdapter extends AppImpl {

    public StateAdapter(State state) {
        super(state.getClass(), state);
    }
}
