package io.deephaven.plugin;

import io.deephaven.plugin.app.App;
import io.deephaven.plugin.type.ObjectType;

/**
 * A plugin is a structured extension point for user-definable behavior.
 *
 * @see ObjectType
 */
public interface Plugin {

    <T, V extends Visitor<T>> T walk(V visitor);

    interface Visitor<T> {
        T visit(ObjectType objectType);
        T visit(App app);
    }
}
