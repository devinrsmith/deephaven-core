package io.deephaven.uri;


import java.net.URI;
import java.nio.file.Path;

public interface ResolvableUri {

    static ResolvableUri of(URI uri) {
        if (LocalUri.isWellFormed(uri)) {
            return LocalUri.of(uri);
        }
        if (RemoteUri.isWellFormed(uri)) {
            return RemoteUri.of(uri);
        }
        if (RawUri.isWellFormed(uri)) {
            return RawUri.of(uri);
        }
        throw new IllegalArgumentException(String.format("Unable to make URI resolvable, '%s'", uri));
    }

    URI toUri();

    String scheme();

    /**
     * Relative path of the different parts.
     *
     * @return the relative path of the parts
     */
    Path toParts();

    RemoteUri target(DeephavenTarget target);

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(LocalUri localUri);

        void visit(RemoteUri remoteUri);

        void visit(URI uri);
    }
}
