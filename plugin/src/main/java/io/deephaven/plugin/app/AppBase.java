package io.deephaven.plugin.app;

import java.util.Objects;

public abstract class AppBase implements App {

    private final String id;

    private final String name;

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

    @Override
    public final <T, V extends Visitor<T>> T walk(V visitor) {
        return visitor.visit(this);
    }
}
