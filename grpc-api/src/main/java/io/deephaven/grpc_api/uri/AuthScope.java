package io.deephaven.grpc_api.uri;

import java.util.Objects;
import java.util.Optional;

public final class AuthScope<T> {

    public static <T> AuthScope<T> readGlobal() {
        return new AuthScope<>(null, true);
    }

    public static <T> AuthScope<T> writeGlobal() {
        return new AuthScope<>(null, false);
    }

    public static <T> AuthScope<T> read(T path) {
        return new AuthScope<>(Objects.requireNonNull(path), true);
    }

    public static <T> AuthScope<T> write(T path) {
        return new AuthScope<>(Objects.requireNonNull(path), false);
    }

    private final T path;
    private final boolean readOnly;

    AuthScope(T path, boolean readOnly) {
        this.path = path;
        this.readOnly = readOnly;
    }

    public boolean isWrite() {
        return !readOnly;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean isGlobal() {
        return path == null;
    }

    public Optional<T> path() {
        return Optional.ofNullable(path);
    }
}
