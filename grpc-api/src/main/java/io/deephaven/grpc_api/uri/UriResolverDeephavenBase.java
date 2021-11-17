package io.deephaven.grpc_api.uri;

import io.deephaven.uri.DeephavenUriBase;
import io.deephaven.util.auth.AuthContext;

import java.net.URI;
import java.util.Objects;
import java.util.function.Function;

public abstract class UriResolverDeephavenBase<U extends DeephavenUriBase> implements UriResolver {

    public interface Config<U extends DeephavenUriBase> {

        boolean isEnabled(AuthContext auth);

        boolean isEnabled(AuthContext auth, U uri);

        String helpEnable(AuthContext auth);

        String helpEnable(AuthContext auth, U uri);

        Object resolve(AuthContext auth, U uri) throws InterruptedException;
    }

    private final Config<U> config;
    private final Function<URI, U> f;

    public UriResolverDeephavenBase(Config<U> config, Function<URI, U> f) {
        this.config = Objects.requireNonNull(config);
        this.f = Objects.requireNonNull(f);
    }

    @Override
    public final Object resolve(URI uri) throws InterruptedException {
        return config.resolve(null, f.apply(uri));
    }

    @Override
    public final Object resolveSafely(AuthContext auth, URI uri) throws InterruptedException {
        Objects.requireNonNull(auth);
        if (!config.isEnabled(auth)) {
            throw new UnsupportedOperationException(
                    String.format("Resolver is disabled. %s", config.helpEnable(auth)));
        }
        final U u = f.apply(uri);
        if (!config.isEnabled(auth, u)) {
            throw new UnsupportedOperationException(String.format("Resolver is disable for URI '%s'. %s", uri, config.helpEnable(auth, u)));
        }
        return config.resolve(auth, u);
    }
}
