package io.deephaven.uri;

import io.deephaven.grpc_api.uri.UriResolver;
import io.deephaven.grpc_api.uri.UriRouterInstance;

import java.net.URI;

/**
 * The top-level entrypoint for resolving {@link URI URIs} into {@link Object objects}. Uses the global URI resolvers
 * instance from {@link UriRouterInstance#get()}.
 *
 * <p>
 * The exact logic will depend on which {@link UriResolver URI resolvers} are installed.
 *
 * @see StructuredUri structured URI
 */
public class ResolveTools {

    /**
     * Resolves the {@code uri} into an object.
     *
     * @param uri the URI
     * @return the object
     */
    public static Object resolve(String uri) throws InterruptedException {
        return resolve(URI.create(uri));
    }

    /**
     * Resolves the {@code uri} into an object.
     *
     * @param uri the URI
     * @return the object
     */
    public static Object resolve(URI uri) throws InterruptedException {
        return UriRouterInstance.get().resolve(uri);
    }
}
