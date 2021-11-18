package io.deephaven.grpc_api.uri;

import io.deephaven.util.auth.AuthContext;

import java.net.URI;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * A URI resolver resolves {@link URI URIs} into {@link Object objects}.
 */
public interface UriResolver {

    /**
     * The supported schemes.
     *
     * @return the schemes
     */
    Set<String> schemes();

    /**
     * Returns {@code true} if {@code this} could be expected to {@link #resolve(URI)} the {@code uri}. A {@code uri}
     * that generally matches the pattern, but would actually throw an error during {@link #resolve(URI)}, can return
     * {@code true} here, if the expectation is that a more tailored error message will be thrown during
     * {@link #resolve(URI)}.
     *
     * <p>
     * Resolvability does <b>not</b> take into account safety concerns.
     *
     * @param uri the uri
     * @return {@code true} if this resolver expects to be able to resolve {@code uri}
     */
    boolean isResolvable(URI uri);

    /**
     * Resolve {@code uri} into an object.
     *
     * @param uri the URI
     * @return the object
     */
    Object resolve(URI uri) throws InterruptedException;

    /**
     * Resolve {@code uri} into an object, safely, according to the conditions set for {@code this} specific resolver.
     *
     * @param auth the auth context
     * @param uri the URI
     * @return the object
     */
    Object resolveSafely(AuthContext auth, URI uri) throws InterruptedException;

    void forAllUris(BiConsumer<URI, Object> consumer);

    void forAllUrisSafely(AuthContext auth, BiConsumer<URI, Object> consumer);
}
