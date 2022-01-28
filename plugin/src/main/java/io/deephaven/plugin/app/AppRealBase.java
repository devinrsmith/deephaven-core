package io.deephaven.plugin.app;

public abstract class AppRealBase implements App {

    @Override
    public final <T, V extends Visitor<T>> T walk(V visitor) {
        return visitor.visit(this);
    }
}
