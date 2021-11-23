package io.deephaven.grpc_api.uri;


import java.util.Objects;
import java.util.Optional;

public class Authorization<T> {
    public static <T> Authorization<T> allow(AuthScope<T> scope) {
        return new Authorization<>(scope, null);
    }

    public static <T> Authorization<T> deny(AuthScope<T> scope, String reason) {
        return new Authorization<>(scope, Objects.requireNonNull(reason));
    }

    private final AuthScope<T> scope;
    private final String reason;

    Authorization(AuthScope<T> scope, String reason) {
        this.scope = Objects.requireNonNull(scope);
        this.reason = reason;
    }

    public boolean isAllowed() {
        return reason == null;
    }

    public boolean isDenied() {
        return reason != null;
    }

    public AuthScope<T> scope() {
        return scope;
    }

    public Optional<String> reason() {
        return Optional.ofNullable(reason);
    }
}
