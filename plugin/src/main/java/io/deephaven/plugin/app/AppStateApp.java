package io.deephaven.plugin.app;

import java.util.Objects;

public class AppStateApp extends AppBase {

    private final App.State state;

    protected AppStateApp(String id, String name, App.State state) {
        super(id, name);
        this.state = Objects.requireNonNull(state);
    }

    @Override
    public void execute(Consumer consumer) {
        state.insertInto(consumer);
    }
}
