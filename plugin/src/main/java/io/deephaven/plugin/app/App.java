package io.deephaven.plugin.app;

import io.deephaven.plugin.Plugin;

/**
 * Alpha interface
 */
public interface App extends Plugin, State {
    String id();

    String name();
}
