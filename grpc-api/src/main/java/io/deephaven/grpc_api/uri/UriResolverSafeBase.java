package io.deephaven.grpc_api.uri;

import io.deephaven.util.auth.AuthContext;

import java.net.URI;

public abstract class UriResolverSafeBase implements UriResolver {

    @Override
    public final Object resolveSafely(AuthContext auth, URI uri) throws InterruptedException {
        return resolve(uri);
    }
}
