package io.deephaven.uri;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * A Deephaven URI represents a structured link for resolving Deephaven resources. It is composed of an optional
 * {@link #target() target} and a {@link #path() path}. It is meant to be completely representable by a {@link URI}
 * which may be shared.
 */
public interface DeephavenUri {

    Path APPLICATION = Paths.get("a");

    Path FIELD = Paths.get("f");

    Path QUERY_SCOPE = Paths.get("s");

    Path TLS_PROXY = Paths.get(DeephavenTarget.TLS_SCHEME);

    Path PLAIN_PROXY = Paths.get(DeephavenTarget.PLAINTEXT_SCHEME);

    /**
     * Parses the {@code uri} into a Deephaven URI.
     *
     * <p>
     * Equivalent to {@code of(URI.create(uri))}.
     *
     * @param uri the uri
     * @return the Deephaven URI
     */
    static DeephavenUri of(String uri) {
        return of(URI.create(uri));
    }

    /**
     * Parses the {@code uri} into a Deephaven URI.
     *
     * @param uri the uri
     * @return the Deephaven URI
     */
    static DeephavenUri of(URI uri) {
        if (!DeephavenTarget.isValidScheme(uri.getScheme())) {
            throw new IllegalArgumentException(String.format("Invalid Deephaven URI scheme '%s'", uri.getScheme()));
        }
        if (uri.isOpaque()) {
            throw new IllegalArgumentException("Deephaven URIs are not opaque");
        }
        if (uri.getPath() == null || uri.getPath().charAt(0) != '/') {
            throw new IllegalArgumentException("Deephavhen URI path must be absolute");
        }
        if (uri.getUserInfo() != null) {
            throw new IllegalArgumentException("Deephaven URI does not support user info at this time");
        }
        if (uri.getQuery() != null) {
            throw new IllegalArgumentException("Deephaven URI does not support query params at this time");
        }
        if (uri.getFragment() != null) {
            throw new IllegalArgumentException("Deephaven URI does not support fragments at this time");
        }
        // Strip absolute path '/' from URI
        final Path path = Paths.get(uri.getPath().substring(1));
        if (uri.getHost() == null) {
            return of(path);
        }
        // Don't need strict here since we are strict above
        final DeephavenTarget target = DeephavenTarget.of(uri, false);
        return of(target, path);
    }

    /**
     * Creates a local Deephaven URI by parsing the given {@code path}.
     *
     * @param path the path
     * @return the local Deephaven URI
     */
    static DeephavenUri of(Path path) {
        return UriHelper.of(path);
    }

    /**
     * Creates a remote Deephaven URI from the given {@code target} and by parsing the {@code path}.
     *
     * @param target the target
     * @param path the path
     * @return the remote Deephaven URI
     */
    static DeephavenUri of(DeephavenTarget target, Path path) {
        return UriHelper.of(target, path);
    }

    /**
     * The optional target.
     *
     * @return the target
     */
    Optional<DeephavenTarget> target();

    /**
     * The path.
     *
     * @return the path
     */
    Path path();

    /**
     * The Deephaven URI as a URI.
     *
     * @return the URI
     */
    URI toUri();

    /**
     * A Deephaven URI is local when {@link #target()} is not present.
     *
     * @return true iff local
     */
    default boolean isLocal() {
        return !target().isPresent();
    }

    /**
     * A Deephaven URI is remote when {@link #target()} is present.
     *
     * @return true iff remote
     */
    default boolean isRemote() {
        return target().isPresent();
    }

    <V extends Visitor> V walk(V visitor);

    DeephavenUri proxyVia(DeephavenTarget proxyTarget);

    interface Visitor {
        void visit(DeephavenUriField field);

        void visit(DeephavenUriApplicationField applicationField);

        void visit(DeephavenUriQueryScope queryScope);

        void visit(DeephavenUriProxy proxy);
    }
}
