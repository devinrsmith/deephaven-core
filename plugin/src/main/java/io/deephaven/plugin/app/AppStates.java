package io.deephaven.plugin.app;

import java.util.Arrays;
import java.util.Objects;

public class AppStates extends AppBase {

    private final Iterable<? extends App.State> states;

    public AppStates(String id, String name, App.State... states) {
        this(id, name, Arrays.asList(states));
    }

    public AppStates(String id, String name, Iterable<? extends App.State> states) {
        super(id, name);
        this.states = Objects.requireNonNull(states);
    }

    @Override
    public final void execute(Consumer consumer) {
        for (State state : states) {
            state.insertInto(consumer);
        }
    }
}
