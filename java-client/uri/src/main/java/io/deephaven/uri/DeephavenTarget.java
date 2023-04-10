/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.uri;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalInt;

/**
 * A Deephaven target represents the information necessary to establish a connection to a remote Deephaven service.
 *
 * <p>
 * A Deephaven target has a {@link #isSecure() secure flag}, {@link #host() host}, and optional {@link #port() port}.
 * When the port is not specified, it's up to the client to determine the appropriate port (possibly by using a default
 * port or discovering the appropriate port to use).
 *
 * @see #of(URI) parsing logic
 */
@Immutable
@BuildableStyle
public abstract class DeephavenTarget {

    private static final String DHQUERY = "dhquery";
    private static final String DHFRAGMENT = "dhfragment";

    public static Builder builder() {
        return ImmutableDeephavenTarget.builder();
    }

    /**
     * Returns true if the scheme is valid for a Deephaven target.
     *
     * <p>
     * The valid schemes are {@value DeephavenUri#SECURE_SCHEME} and {@value DeephavenUri#PLAINTEXT_SCHEME}.
     *
     * @param scheme the scheme
     * @return true iff scheme is valid for Deephaven target
     *
     */
    public static boolean isValidScheme(String scheme) {
        return DeephavenUri.isValidScheme(scheme);
    }

    public static boolean isWellFormed(URI uri) {
        return isValidScheme(uri.getScheme())
                && uri.getHost() != null
                && !uri.isOpaque()
                && uri.getPath().isEmpty()
                && uri.getQuery() == null
                && uri.getUserInfo() == null;
    }

    /**
     * Parses the {@code targetUri} into a Deephaven target.
     *
     * <p>
     * The valid formats include {@code dh://host}, {@code dh://host:port}, {@code dh+plain://host}, and
     * {@code dh+plain://host:port}.
     *
     * @param targetUri the target URI
     * @return the Deephaven target
     */
    public static DeephavenTarget of(URI targetUri) {
        if (!isWellFormed(targetUri)) {
            throw new IllegalArgumentException(String.format("Invalid target Deephaven URI '%s'", targetUri));
        }
        return from(targetUri);
    }

    /**
     * Parses the {@code uri} into a Deephaven target, without strict URI checks. Useful when parsing a Deephaven target
     * as part of a {@link StructuredUri structured URI}.
     *
     * @param uri the URI
     * @return the Deephaven target
     */
    public static DeephavenTarget from(URI uri) {
        final String scheme = uri.getScheme();
        final int port = uri.getPort();
        Builder builder = builder().host(uri.getHost());
        switch (scheme) {
            case DeephavenUri.SECURE_SCHEME:
                builder.isSecure(true);
                break;
            case DeephavenUri.PLAINTEXT_SCHEME:
                builder.isSecure(false);
                break;
            default:
                throw new IllegalArgumentException(String.format("Invalid Deephaven target scheme '%s'", scheme));
        }
        if (port != -1) {
            builder.port(port);
        }
        final String fragment = uri.getFragment();
        if (fragment != null) {
            putHeaders(builder, fragment);
        }
        return builder.build();
    }

    /**
     * The secure flag, typically representing Transport Layer Security (TLS).
     *
     * @return true if secure
     */
    public abstract boolean isSecure();

    /**
     * The host or IP address.
     *
     * @return the host
     */
    public abstract String host();

    /**
     * The optional port.
     *
     * @return the port
     */
    public abstract OptionalInt port();

    /**
     * The headers necessary to establish a connection.
     *
     * <p>
     * When present, these are typically entries that are necessary for intermediate proxies to correctly route the gRPC
     * connection.
     *
     * @return the headers
     */
    public abstract Map<String, String> headers();

    /**
     * The target as a URI.
     *
     * @return the URI
     */
    public final URI toURI() {
        try {
            return toURIImpl();
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * The target as a URI string.
     *
     * @return the URI string
     */
    @Override
    public final String toString() {
        return toURI().toString();
    }

    @Check
    final void checkURI() {
        final URI uri;
        try {
            uri = toURIImpl();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        // Will cause URI exception if port is invalid too
        if (!host().equals(uri.getHost())) {
            throw new IllegalArgumentException(String.format("Invalid host '%s'", host()));
        }
    }

    @Check
    final void checkPort() {
        if (port().isPresent()) {
            if (port().getAsInt() <= 0 || port().getAsInt() > 65535) {
                throw new IllegalArgumentException("Port must be in range [1, 65535]");
            }
        }
    }

    public interface Builder {

        Builder host(String host);

        Builder port(int port);

        Builder isSecure(boolean isSecure);

        Builder putHeaders(String key, String value);

        Builder putAllHeaders(Map<String, ? extends String> entries);

        DeephavenTarget build();
    }

    private URI toURIImpl() throws URISyntaxException {
        return new URI(
                isSecure() ? DeephavenUri.SECURE_SCHEME : DeephavenUri.PLAINTEXT_SCHEME,
                null,
                host(),
                port().orElse(-1),
                null,
                null,
                headersToFragment());
    }

    private String headersToFragment() {
        if (headers().isEmpty()) {
            return null;
        }
        final StringBuilder sb = new StringBuilder();
        int i = 0;
        for (Entry<String, String> e : headers().entrySet()) {
            if (i > 0) {
                sb.append('&');
            }
            if (DHFRAGMENT.equals(e.getKey())) {
                sb.append(e.getValue());
            } else {
                sb.append(e.getKey()).append('=').append(e.getValue());
            }
            ++i;
        }
        return sb.toString();
    }

    private static void putHeaders(Builder builder, String fragment) {
        for (String entry : fragment.split("&")) {
            final String[] split = entry.split("=");
            if (split.length == 1) {
                builder.putHeaders(DHFRAGMENT, split[0]);
            } else if (split.length == 2) {
                builder.putHeaders(split[0], split[1]);
            } else {
                throw new IllegalArgumentException("todo");
            }
        }
    }
}
