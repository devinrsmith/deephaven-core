package io.deephaven.grpc_api.uri;

import io.deephaven.util.auth.AuthContext;

import java.net.URI;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * An opinionated structuring to implementing {@link UriResolver} in a safe manner.
 * 
 * @param <T> the parsed URI
 */
public abstract class UriResolverBase<T> implements UriResolver {

    public abstract T adaptToPath(URI uri);

    public abstract URI adaptToUri(T path);

    public abstract Authorization<T> authorization(AuthorizationScope<T> scope, AuthContext context);

    public abstract Object resolvePath(T path) throws InterruptedException;

    public abstract void forAllPaths(BiConsumer<T, Object> consumer);

    public <O> Consumer<O> publishTarget(T item) {
        throw new UnsupportedOperationException("Does not support publish");
    }

    @Override
    public final Object resolve(URI uri) throws InterruptedException {
        return resolvePath(adaptToPath(uri));
    }

    @Override
    public final Object resolveSafely(AuthContext auth, URI uri) throws InterruptedException {
        check(authorization(AuthorizationScope.readGlobal(), auth));
        final T path = adaptToPath(uri);
        check(authorization(AuthorizationScope.read(path), auth));
        return resolvePath(path);
    }

    @Override
    public final <O> Consumer<O> publishTarget(AuthContext auth, URI uri) {
        check(authorization(AuthorizationScope.writeGlobal(), auth));
        final T path = adaptToPath(uri);
        check(authorization(AuthorizationScope.write(path), auth));
        return publishTarget(path);
    }

    @Override
    public final void forAllUris(BiConsumer<URI, Object> consumer) {
        forAllPaths((path, obj) -> consumer.accept(adaptToUri(path), obj));
    }

    @Override
    public final void forAllUrisSafely(AuthContext auth, BiConsumer<URI, Object> consumer) {
        if (!authorization(AuthorizationScope.readGlobal(), auth).isAllowed()) {
            return;
        }
        forAllPaths((path, obj) -> {
            if (authorization(AuthorizationScope.read(path), auth).isAllowed()) {
                consumer.accept(adaptToUri(path), obj);
            }
        });
    }

    private static void check(Authorization<?> authorization) {
        if (authorization.isDenied()) {
            throw new UnsupportedOperationException(authorization.reason().orElseThrow(IllegalStateException::new));
        }
    }
}
