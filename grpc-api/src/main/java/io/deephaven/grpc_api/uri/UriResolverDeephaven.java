package io.deephaven.grpc_api.uri;

import io.deephaven.uri.DeephavenUriBase;
import io.deephaven.util.auth.AuthContext;

import java.net.URI;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

public abstract class UriResolverDeephaven<U extends DeephavenUriBase> implements UriResolver {

    public abstract U adapt(URI uri);

    public abstract boolean isEnabled(AuthContext auth);

    public abstract boolean isEnabled(AuthContext auth, U uri);

    public abstract String helpEnable(AuthContext auth);

    public abstract String helpEnable(AuthContext auth, U uri);

    public abstract Object resolve(U uri) throws InterruptedException;

    @Override
    public final Object resolve(URI uri) throws InterruptedException {
        return resolve(adapt(uri));
    }

    @Override
    public final Object resolveSafely(AuthContext auth, URI uri) throws InterruptedException {
        Objects.requireNonNull(auth);
        if (!isEnabled(auth)) {
            throw new UnsupportedOperationException(String.format("Resolver is not enabled. %s", helpEnable(auth)));
        }
        final U u = adapt(uri);
        if (!isEnabled(auth, u)) {
            throw new UnsupportedOperationException(
                    String.format("Resolver is not enabled for URI '%s'. %s", uri, helpEnable(auth, u)));
        }
        return resolve(u);
    }
}
