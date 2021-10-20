package io.deephaven.uri;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * A local Deephaven URI represents a structured link for resolving local Deephaven resources.
 */
public interface LocalUri extends ResolvableUri {

    /**
     * The scheme for local Deephaven URIs, {@code local}.
     */
    String LOCAL_SCHEME = "local";

    /**
     * Parses the {@code uri} into a local Deephaven URI.
     *
     * <p>
     * Equivalent to {@code of(URI.create(uri))}.
     *
     * @param uri the uri
     * @return the local Deephaven URI
     */
    static LocalUri of(String uri) {
        return of(URI.create(uri));
    }

    static boolean isValidScheme(String scheme) {
        return LOCAL_SCHEME.equals(scheme);
    }

    static boolean isWellFormed(URI uri) {
        return isValidScheme(uri.getScheme())
                && uri.getHost() == null
                && !uri.isOpaque()
                && uri.getPath() != null
                && uri.getPath().charAt(0) == '/'
                && uri.getUserInfo() == null
                && uri.getFragment() == null;
    }

    /**
     * Parses the {@code uri} into a Deephaven URI.
     *
     * @param uri the uri
     * @return the Deephaven URI
     */
    static LocalUri of(URI uri) {
        if (!isValidScheme(uri.getScheme())) {
            throw new IllegalArgumentException(
                    String.format("Invalid local Deephaven URI scheme '%s'", uri.getScheme()));
        }
        if (uri.getHost() != null) {
            throw new IllegalArgumentException("Local Deephaven URIs must not have host");
        }
        if (uri.isOpaque()) {
            throw new IllegalArgumentException("Local Deephaven URIs are not opaque");
        }
        if (uri.getPath() == null || uri.getPath().charAt(0) != '/') {
            throw new IllegalArgumentException("Local Deephavhen URI path must be absolute");
        }
        if (uri.getUserInfo() != null) {
            throw new IllegalArgumentException("Local Deephaven URI does not support user info at this time");
        }
        if (uri.getQuery() != null) {
            throw new IllegalArgumentException("Local Deephaven URI does not support query params at this time");
        }
        if (uri.getFragment() != null) {
            throw new IllegalArgumentException("Local Deephaven URI does not support fragments at this time");
        }
        final String relativePath = uri.getPath().substring(1);
        return parse(Paths.get(uri.getScheme()).resolve(relativePath));
    }

    static LocalUri parse(String scheme, String rawPath) {
        return of(URI.create(String.format("%s:///%s", scheme, rawPath)));
    }

    static LocalUri parse(Path path) {
        if (path.getNameCount() == 0) {
            throw new IllegalArgumentException("Empty path");
        }
        final String scheme = path.getName(0).toString();
        if (!isValidScheme(scheme)) {
            throw new IllegalArgumentException(
                    String.format("Invalid local Deephaven URI scheme '%s'", scheme));
        }
        path = path.subpath(1, path.getNameCount());
        if (LocalQueryScopeUri.isMatch(path)) {
            return LocalQueryScopeUri.of(path);
        }
        if (LocalApplicationUri.isMatch(path)) {
            return LocalApplicationUri.of(path);
        }
        if (LocalFieldUri.isMatch(path)) {
            return LocalFieldUri.of(path);
        }
        throw new IllegalArgumentException(String.format("Invalid path, '%s'", path));
    }

    /**
     * The local path, does not contain scheme.
     *
     * @return the local path
     */
    Path localPath();

    <V extends Visitor> V walk(V visitor);

    interface Visitor {

        void visit(LocalFieldUri fieldUri);

        void visit(LocalApplicationUri applicationField);

        void visit(LocalQueryScopeUri queryScope);
    }
}
