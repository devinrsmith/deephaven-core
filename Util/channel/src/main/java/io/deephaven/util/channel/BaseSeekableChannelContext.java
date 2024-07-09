//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class BaseSeekableChannelContext implements SeekableChannelContext {

    /**
     * A sentinel value to indicate that a resource is {@code null}.
     */
    private static final Object NULL_SENTINEL = new Object();

    /**
     * An empty cache of resources.
     */
    private static final Map<Key<?>, Object> EMPTY_CACHE = Map.of();

    /**
     * A cache of opaque resource objects hosted by this context.
     */
    private Map<Key<?>, Object> resourceCache = EMPTY_CACHE;

    @Override
    @Nullable
    public final <T> T cache(final Key<T> key, @NotNull final Supplier<T> supplier) {
        final Map<Key<?>, Object> localResourceCache = resourceCache == EMPTY_CACHE
                ? resourceCache = new HashMap<>(1)
                : resourceCache;
        // noinspection unchecked
        T resource = (T) localResourceCache.get(key);
        if (resource == NULL_SENTINEL) {
            return null;
        }
        if (resource == null) {
            resourceCache.put(key, (resource = supplier.get()) == null ? NULL_SENTINEL : resource);
        }
        return resource;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void close() {
        SafeCloseable.closeAll(resourceCache.values().stream().filter(BaseSeekableChannelContext::isAutoCloseable));
        resourceCache = EMPTY_CACHE;
    }

    private static boolean isAutoCloseable(Object x) {
        return x instanceof AutoCloseable;
    }
}
