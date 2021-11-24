package io.deephaven.grpc_api.uri;

import io.deephaven.util.auth.AuthContext;

import java.net.URI;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

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

    public abstract void forPaths(Predicate<T> predicate, BiConsumer<T, Object> consumer);

    public <O> Consumer<O> publishTarget(T path) {
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
    public final <O> Consumer<O> publish(URI uri) throws UnsupportedOperationException {
        final T path = adaptToPath(uri);
        return publishTarget(path);
    }

    @Override
    public final <O> Consumer<O> publishSafely(AuthContext auth, URI uri) {
        check(authorization(AuthorizationScope.writeGlobal(), auth));
        final T path = adaptToPath(uri);
        check(authorization(AuthorizationScope.write(path), auth));
        return publishTarget(path);
    }

    @Override
    public final void forAllUris(BiConsumer<URI, Object> consumer) {
        forAllPaths(new Adapter(null, consumer));
    }

    @Override
    public final void forAllUrisSafely(AuthContext auth, BiConsumer<URI, Object> consumer) {
        if (!authorization(AuthorizationScope.readGlobal(), auth).isAllowed()) {
            return;
        }
        final AuthorizationAdapter adapter = new AuthorizationAdapter(auth, null, consumer);
        forPaths(adapter, adapter);
    }

    @Override
    public final void forUris(Predicate<URI> predicate, BiConsumer<URI, Object> consumer) {
        final Adapter adapter = new Adapter(Objects.requireNonNull(predicate), consumer);
        forPaths(adapter, adapter);
    }

    @Override
    public final void forUrisSafely(AuthContext auth, Predicate<URI> predicate, BiConsumer<URI, Object> consumer) {
        Objects.requireNonNull(predicate);
        if (!authorization(AuthorizationScope.readGlobal(), auth).isAllowed()) {
            return;
        }
        final AuthorizationAdapter adapter = new AuthorizationAdapter(auth, predicate, consumer);
        forPaths(adapter, adapter);
    }

    private static void check(Authorization<?> authorization) {
        if (authorization.isDenied()) {
            throw new UnsupportedOperationException(authorization.reason().orElseThrow(IllegalStateException::new));
        }
    }

    private class Adapter implements Predicate<T>, BiConsumer<T, Object> {
        private final Predicate<URI> predicate;
        private final BiConsumer<URI, Object> delegate;

        Adapter(Predicate<URI> predicate, BiConsumer<URI, Object> delegate) {
            this.predicate = predicate;
            this.delegate = Objects.requireNonNull(delegate);
        }

        @Override
        public boolean test(T path) {
            return predicate == null || predicate.test(adaptToUri(path));
        }

        @Override
        public void accept(T path, Object value) {
            delegate.accept(adaptToUri(path), value);
        }
    }

    private class AuthorizationAdapter implements Predicate<T>, BiConsumer<T, Object> {
        private final AuthContext authContext;
        private final Predicate<URI> predicate;
        private final BiConsumer<URI, Object> delegate;

        AuthorizationAdapter(AuthContext authContext, Predicate<URI> predicate, BiConsumer<URI, Object> delegate) {
            this.predicate = predicate;
            this.authContext = Objects.requireNonNull(authContext);
            this.delegate = Objects.requireNonNull(delegate);
        }

        @Override
        public boolean test(T path) {
            return (predicate == null || predicate.test(adaptToUri(path)))
                    && authorization(AuthorizationScope.read(path), authContext).isAllowed();
        }

        @Override
        public void accept(T path, Object value) {
            delegate.accept(adaptToUri(path), value);
        }
    }
}
