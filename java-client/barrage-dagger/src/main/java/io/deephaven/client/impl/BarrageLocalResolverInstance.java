package io.deephaven.client.impl;

import java.util.Objects;

public class BarrageLocalResolverInstance {

    private static BarrageLocalTableResolver resolver;

    public static void init(BarrageLocalTableResolver resolver) {
        synchronized (BarrageLocalResolverInstance.class) {
            if (BarrageLocalResolverInstance.resolver != null) {
                throw new IllegalStateException("Can only set resolver once");
            }
            BarrageLocalResolverInstance.resolver = resolver;
        }
    }

    public static BarrageLocalTableResolver get() {
        return Objects.requireNonNull(resolver);
    }
}
