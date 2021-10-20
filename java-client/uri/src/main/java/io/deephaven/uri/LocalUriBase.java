package io.deephaven.uri;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class LocalUriBase extends ResolvableUriBase implements LocalUri {

    @Override
    public final URI toUri() {
        return URI.create(toString());
    }

    @Override
    public final String scheme() {
        return LOCAL_SCHEME;
    }

    @Override
    public final Path toParts() {
        return Paths.get(LOCAL_SCHEME).resolve(localPath());
    }

    @Override
    public final String toString() {
        return String.format("%s:///%s", LOCAL_SCHEME, localPath());
    }

    @Override
    public final <V extends ResolvableUri.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
