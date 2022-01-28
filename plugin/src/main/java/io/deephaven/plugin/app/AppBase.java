package io.deephaven.plugin.app;

import java.util.Objects;

public abstract class AppBase extends AppRealBase {

    private final String id;

    private final String name;

    public AppBase(Class<? extends AppBase> clazz) {
        this(clazz.getName(), clazz.getSimpleName());
    }

    public AppBase(String id, String name) {
        this.id = Objects.requireNonNull(id);
        this.name = Objects.requireNonNull(name);
    }

    @Override
    public final String id() {
        return id;
    }

    @Override
    public final String name() {
        return name;
    }
}
