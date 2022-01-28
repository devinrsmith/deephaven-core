package io.deephaven.plugin.app;

public abstract class ApplicationDelegate extends AppBase {

    private final State state;

    public ApplicationDelegate(Class<? extends ApplicationDelegate> clazz, State state) {
        super(clazz);
        this.state = state;
    }

    public ApplicationDelegate(Class<? extends ApplicationDelegate> clazz, State... states) {
        this(clazz, new States(states));
    }

    @Override
    public final void insertInto(Consumer consumer) {
        state.insertInto(consumer);
    }
}
