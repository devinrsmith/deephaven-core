package io.deephaven.uri;

import java.util.Objects;

public class TableResolverInstance {
    private static TableResolver resolver;

    public static void init(TableResolver instance) {
        synchronized (TableResolverInstance.class) {
            if (resolver != null) {
                throw new IllegalStateException("Can only initialize TableResolverInstance once");
            }
            resolver = instance;
        }
    }

    public static TableResolver get() {
        return Objects.requireNonNull(resolver);
    }
}
