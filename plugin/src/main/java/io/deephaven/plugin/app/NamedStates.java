package io.deephaven.plugin.app;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

public class NamedStates implements State {

    public static NamedStates of(String name, State state) {
        return new NamedStates(Map.of(name, state));
    }

    public static NamedStates of(String name, State... states) {
        return new NamedStates(Map.of(name, new States(states)));
    }

    private final Map<String, State> states;

    public NamedStates(Map<String, State> states) {
        this.states = Objects.requireNonNull(states);
    }

    @Override
    public final void insertInto(Consumer consumer) {
        for (Entry<String, State> e : states.entrySet()) {
            consumer.set(e.getKey(), e.getValue());
        }
    }
}
