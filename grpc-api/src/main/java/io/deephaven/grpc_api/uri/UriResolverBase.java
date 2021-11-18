package io.deephaven.grpc_api.uri;

import io.deephaven.util.auth.AuthContext;

import java.net.URI;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * An opinionated structuring to implementing {@link UriResolver} in a safe manner.
 * 
 * @param <T> the parsed URI
 */
public abstract class UriResolverBase<T> implements UriResolver {

    public abstract T adaptToItem(URI uri);

    public abstract URI adaptToUri(T item);

    public abstract boolean isEnabled(AuthContext auth);

    public abstract boolean isEnabled(AuthContext auth, T item);

    public abstract String helpEnable(AuthContext auth);

    public abstract String helpEnable(AuthContext auth, T item);

    public abstract Object resolveItem(T item) throws InterruptedException;

    public abstract void forAllItems(BiConsumer<T, Object> consumer);

    public <O> Consumer<O> publishTarget(T item) {
        throw new UnsupportedOperationException("Does not support publish");
    }

    @Override
    public final Object resolve(URI uri) throws InterruptedException {
        return resolveItem(adaptToItem(uri));
    }

    @Override
    public final Object resolveSafely(AuthContext auth, URI uri) throws InterruptedException {
        if (!isEnabled(auth)) {
            throw new UnsupportedOperationException(String.format("Resolver is not enabled. %s", helpEnable(auth)));
        }
        final T item = adaptToItem(uri);
        if (!isEnabled(auth, item)) {
            throw new UnsupportedOperationException(
                    String.format("Resolver is not enabled for URI '%s'. %s", uri, helpEnable(auth, item)));
        }
        return resolveItem(item);
    }

    @Override
    public final <O> Consumer<O> publishTarget(AuthContext auth, URI uri) {
        // todo: separate auth for write vs read
        if (!isEnabled(auth)) {
            throw new UnsupportedOperationException(String.format("Resolver is not enabled. %s", helpEnable(auth)));
        }
        final T item = adaptToItem(uri);
        if (!isEnabled(auth, item)) {
            throw new UnsupportedOperationException(String.format("Resolver is not enabled for URI '%s'. %s", uri, helpEnable(auth, item)));
        }
        return publishTarget(item);
    }

    @Override
    public final void forAllUris(BiConsumer<URI, Object> consumer) {
        forAllItems((item, obj) -> consumer.accept(adaptToUri(item), obj));
    }

    @Override
    public final void forAllUrisSafely(AuthContext auth, BiConsumer<URI, Object> consumer) {
        if (!isEnabled(auth)) {
            return;
        }
        forAllItems((item, obj) -> {
            if (isEnabled(auth, item)) {
                consumer.accept(adaptToUri(item), obj);
            }
        });
    }
}
