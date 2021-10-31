package io.deephaven.grpc_api.uri;

import java.net.URI;

public abstract class UriResolverSafeBase implements UriResolver {

    @Override
    public final Object resolveSafely(URI uri) throws InterruptedException {
        return resolve(uri);
    }
}
