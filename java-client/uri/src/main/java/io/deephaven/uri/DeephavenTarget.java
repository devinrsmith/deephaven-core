package io.deephaven.uri;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.net.URI;
import java.util.OptionalInt;

/**
 * A Deephaven target represents the information necessary to establish a connection to a Deephaven service. There is a
 * one-to-one mapping between a Deephaven target and its valid {@link #toUri() URI}.
 *
 * <p>
 * A Deephaven target has a {@link #scheme() scheme}, {@link #host() host}, and optional {@link #port() port}.
 *
 * <p>
 * The scheme must be {@code dh}, for TLS; or {@code dh-plain}, for plaintext.
 */
@Immutable
@BuildableStyle
public abstract class DeephavenTarget {

    public static final String TLS_SCHEME = "dh";

    public static final String PLAINTEXT_SCHEME = "dh-plain";

    public static final String LOCAL_SCHEME = TLS_SCHEME;

    public static boolean isValidScheme(String scheme) {
        return TLS_SCHEME.equals(scheme) || PLAINTEXT_SCHEME.equals(scheme);
    }

    public static Builder builder() {
        return ImmutableDeephavenTarget.builder();
    }

    /**
     * Parses the {@code targetUri} into a Deephaven target.
     *
     * <p>
     * Equivalent to {@code of(URI.create(targetUri), true)}.
     *
     * @param targetUri the target uri
     * @return the Deephaven target
     */
    public static DeephavenTarget of(String targetUri) {
        return of(URI.create(targetUri), true);
    }

    /**
     * Parses the {@code targetUri} into a Deephaven target. When strict, the parsing ensures there aren't any
     * extraneous parts of the {@code targetUri}.
     *
     * @param targetUri the target uri
     * @param strict if the parsing should be strict
     * @return the target
     */
    public static DeephavenTarget of(URI targetUri, boolean strict) {
        Builder builder = builder();
        if (targetUri.getScheme() == null) {
            throw new IllegalArgumentException("Must provide scheme");
        }
        switch (targetUri.getScheme()) {
            case TLS_SCHEME:
                builder.isTLS(true);
                break;
            case PLAINTEXT_SCHEME:
                builder.isTLS(false);
                break;
            default:
                throw new IllegalArgumentException(String.format("Unexpected scheme '%s'", targetUri.getScheme()));
        }
        final String host = targetUri.getHost();
        if (host == null) {
            throw new IllegalArgumentException("Unable to construct target with no host");
        }
        builder.host(host);
        final int port = targetUri.getPort();
        if (port != -1) {
            builder.port(port);
        }
        final DeephavenTarget target = builder.build();
        if (strict) {
            // Need to ensure there isn't "extra" stuff like query params attached.
            // Callers that pre-check parts can skip this.
            if (!targetUri.equals(target.toUri())) {
                throw new IllegalArgumentException(String.format("URI is not a simple target '%s'", targetUri));
            }
        }
        return target;
    }

    /**
     * The scheme.
     *
     * @return the scheme
     */
    public final String scheme() {
        return isTLS() ? TLS_SCHEME : PLAINTEXT_SCHEME;
    }

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
     * The target as a URI.
     *
     * @return the uri
     */
    public final URI toUri() {
        return URI.create(toString());
    }

    @Default
    public boolean isTLS() {
        return "true".equals(System.getProperty("deephaven.uri.tls", "false"));
    }

    public final String authority() {
        return port().isPresent() ? String.format("%s:%d", host(), port().getAsInt()) : host();
    }

    public final URI toUri(String path) {
        return URI.create(String.format("%s://%s/%s", scheme(), authority(), path));
    }

    @Check
    final void checkHostPort() {
        // Will cause URI exception if port is invalid too
        if (!host().equals(toUri().getHost())) {
            throw new IllegalArgumentException(String.format("Invalid host '%s'", host()));
        }
    }

    @Override
    public final String toString() {
        return String.format("%s://%s", scheme(), authority());
    }

    public interface Builder {

        Builder host(String host);

        Builder port(int port);

        Builder isTLS(boolean useTLS);

        DeephavenTarget build();
    }
}
