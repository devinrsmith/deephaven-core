package io.deephaven.plugin.app;

public interface State {
    void insertInto(Consumer consumer);

    interface Consumer {
        void set(String name, Object object);

        void set(String name, Object object, String description);

        void setState(String prefix, State state);
    }
}
