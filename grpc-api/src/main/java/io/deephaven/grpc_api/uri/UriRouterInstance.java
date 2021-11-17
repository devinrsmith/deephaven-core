package io.deephaven.grpc_api.uri;

import java.util.Objects;

public class UriRouterInstance {
    private static UriRouter router;

    public static void init(UriRouter instance) {
        synchronized (UriRouterInstance.class) {
            if (router != null) {
                throw new IllegalStateException("Can only initialize UriRouterInstance once");
            }
            router = instance;
        }
    }

    public static UriRouter get() {
        return Objects.requireNonNull(router);
    }
}
