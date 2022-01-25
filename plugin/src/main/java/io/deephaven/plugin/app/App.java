package io.deephaven.plugin.app;

import io.deephaven.plugin.Plugin;

/**
 * Alpha interface
 */
public interface App extends Plugin {
    String id();

    String name();

    void execute(Consumer consumer);

    interface State {
        void insertInto(Consumer consumer);
    }

    interface Consumer {
        void set(String name, Object object);

        void set(String name, Object object, String description);
    }
}
