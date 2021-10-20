package io.deephaven.uri;

import java.net.URI;

public abstract class DeephavenUriBase implements DeephavenUri {

    @Override
    public final URI toUri() {
        if (isLocal()) {
            return URI.create(String.format("%s:///%s", DeephavenTarget.LOCAL_SCHEME, path()));
        }
        return target()
                .orElseThrow(IllegalStateException::new)
                .toUri(path().toString());
    }

    @Override
    public final DeephavenUri proxyVia(DeephavenTarget proxyTarget) {
        return DeephavenUriProxy.builder()
                .target(proxyTarget)
                .innerUri(this)
                .build();
    }

    @Override
    public final String toString() {
        return toUri().toString();
    }
}
