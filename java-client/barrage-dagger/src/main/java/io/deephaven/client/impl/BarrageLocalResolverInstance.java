package io.deephaven.client.impl;

import java.util.Objects;

public class BarrageLocalResolverInstance {

    private static BarrageLocalResolver resolver;

    public static void init(BarrageLocalResolver resolver) {
        synchronized (BarrageLocalResolverInstance.class) {
            if (BarrageLocalResolverInstance.resolver != null) {
                throw new IllegalStateException("Can only set resolver once");
            }
            BarrageLocalResolverInstance.resolver = resolver;
        }
    }

    public static BarrageLocalResolver get() {
        return Objects.requireNonNull(resolver);
    }
}
