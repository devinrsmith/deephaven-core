package io.deephaven.uri;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.net.URI;
import java.nio.file.Path;

/**
 * A remote Deephaven URI represents a structured link for resolving remote Deephaven resources. Is composed of a
 * {@link #target()} and {@link #uri()}.
 */
@Immutable
@SimpleStyle
public abstract class RemoteUri extends ResolvableUriBase {

    public static RemoteUri of(DeephavenTarget target, ResolvableUri uri) {
        return ImmutableRemoteUri.of(target, uri);
    }

    public static boolean isValidScheme(String scheme) {
        return DeephavenTarget.isValidScheme(scheme);
    }

    static boolean isWellFormed(URI uri) {
        return isValidScheme(uri.getScheme())
                && uri.getHost() != null
                && !uri.isOpaque()
                && uri.getPath() != null
                && uri.getPath().charAt(0) == '/'
                && uri.getUserInfo() == null
                && uri.getFragment() == null;
    }

    public static RemoteUri of(URI uri) {
        if (!DeephavenTarget.isValidScheme(uri.getScheme())) {
            throw new IllegalArgumentException(
                    String.format("Invalid remote Deephaven URI scheme '%s'", uri.getScheme()));
        }
        if (uri.getHost() == null) {
            throw new IllegalArgumentException("Remote Deephaven URIs must have host");
        }
        if (uri.isOpaque()) {
            throw new IllegalArgumentException("Remote Deephaven URIs are not opaque");
        }
        if (uri.getPath() == null || uri.getPath().charAt(0) != '/') {
            throw new IllegalArgumentException("Remote Deephavhen URI path must be absolute");
        }
        if (uri.getUserInfo() != null) {
            throw new IllegalArgumentException("Remote Deephaven URI does not support user info at this time");
        }
        if (uri.getQuery() != null) {
            throw new IllegalArgumentException("Remote Deephaven URI does not support query params at this time");
        }
        if (uri.getFragment() != null) {
            throw new IllegalArgumentException("Remote Deephaven URI does not support fragments at this time");
        }

        // Strip absolute path '/' from URI
        final String rawPath = uri.getRawPath().substring(1);
        final int sep = rawPath.indexOf('/');
        if (sep == -1) {
            throw new IllegalArgumentException("Unable to find scheme / path separator");
        }
        final String remoteScheme = rawPath.substring(0, sep);
        final String remoteRawPath = rawPath.substring(sep + 1);

        // Don't need strict here since we are strict above
        final DeephavenTarget target = DeephavenTarget.of(uri, false);
        return of(target, remoteScheme, remoteRawPath);
    }

    public static RemoteUri parse(String scheme, String rawPath) {
        return of(URI.create(String.format("%s://%s", scheme, rawPath)));
    }

    public static RemoteUri of(DeephavenTarget target, String remoteScheme, String remotePath) {
        if (LocalUri.isValidScheme(remoteScheme)) {
            return LocalUri.parse(remoteScheme, remotePath).target(target);
        }
        if (RemoteUri.isValidScheme(remoteScheme)) {
            return RemoteUri.parse(remoteScheme, remotePath).target(target);
        }
        if (RawUri.isValidScheme(remoteScheme)) {
            return RawUri.parse(remoteScheme, remotePath).target(target);
        }
        throw new IllegalArgumentException();
    }


    @Parameter
    public abstract DeephavenTarget target();

    @Parameter
    public abstract ResolvableUri uri();

    @Override
    public final URI toUri() {
        return target().toUri(uri().toParts().toString());
    }

    @Override
    public final Path toParts() {
        return target().toParts().resolve(uri().toParts());
    }

    @Override
    public final String scheme() {
        return target().scheme();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return super.toString();
    }
}
