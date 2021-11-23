package io.deephaven.grpc_api.uri;

import java.util.Objects;
import java.util.Optional;

public final class AuthorizationScope<T> {

    public static <T> AuthorizationScope<T> readGlobal() {
        return new AuthorizationScope<>(null, true);
    }

    public static <T> AuthorizationScope<T> writeGlobal() {
        return new AuthorizationScope<>(null, false);
    }

    public static <T> AuthorizationScope<T> read(T path) {
        return new AuthorizationScope<>(Objects.requireNonNull(path), true);
    }

    public static <T> AuthorizationScope<T> write(T path) {
        return new AuthorizationScope<>(Objects.requireNonNull(path), false);
    }

    private final T path;
    private final boolean readOnly;

    AuthorizationScope(T path, boolean readOnly) {
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
