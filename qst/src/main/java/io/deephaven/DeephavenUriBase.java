package io.deephaven;

import org.immutables.value.Value.Check;

import java.net.URI;

public abstract class DeephavenUriBase implements DeephavenUriI {

    @Override
    public final URI toUri() {
        if (isLocal()) {
            return URI.create(String.format("%s:///%s", DeephavenTarget.LOCAL_SCHEME, path()));
        }
        return target()
                .orElseThrow(IllegalStateException::new)
                .targetUri()
                .resolve(path().toString());
    }

    @Override
    public final DeephavenUriI proxyVia(String gatewayHost) {
        return DeephavenUriProxy.builder()
                .host(gatewayHost)
                .innerUri(this)
                .build();
    }

    @Override
    public DeephavenUriI proxyVia(String gatewayHost, int gatewayPort) {
        return DeephavenUriProxy.builder()
                .host(gatewayHost)
                .port(gatewayPort)
                .innerUri(this)
                .build();
    }

    @Check
    final void checkHost() {
        if (!host().isPresent()) {
            return;
        }
        final String host = host().get();
        if (host.isEmpty()) {
            throw new IllegalArgumentException("Host must be non-empty");
        }
        final URI uri = URI.create(String.format("dh://%s", host));
        if (!host.equals(uri.getHost())) {
            throw new IllegalArgumentException(String.format("Invalid host '%s'", host));
        }
    }

    @Check
    final void checkPort() {
        if (!port().isPresent()) {
            return;
        }
        if (!host().isPresent()) {
            throw new IllegalArgumentException("Can't specify port without a host");
        }
        final int port = port().getAsInt();
        final URI uri = URI.create(String.format("dh://localhost:%d", port));
        if (port != uri.getPort()) {
            throw new IllegalArgumentException(String.format("Invalid port %d", port));
        }
    }

    @Override
    public final String toString() {
        return toUri().toString();
    }
}
