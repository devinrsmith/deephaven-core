package io.deephaven.plugin.app;

import java.util.Arrays;
import java.util.Objects;

public class States implements State {
    private final Iterable<? extends State> states;

    public States(State... states) {
        this(Arrays.asList(states));
    }

    public States(Iterable<? extends State> states) {
        this.states = Objects.requireNonNull(states);
    }

    @Override
    public final void insertInto(Consumer consumer) {
        for (State state : states) {
            state.insertInto(consumer);
        }
    }
}
