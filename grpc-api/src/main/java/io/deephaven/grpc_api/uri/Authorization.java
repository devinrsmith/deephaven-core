package io.deephaven.grpc_api.uri;


import java.util.Objects;
import java.util.Optional;

public class Authorization<T> {
    public static <T> Authorization<T> allow(AuthorizationScope<T> scope) {
        return new Authorization<>(scope, null);
    }

    public static <T> Authorization<T> deny(AuthorizationScope<T> scope, String reason) {
        return new Authorization<>(scope, Objects.requireNonNull(reason));
    }

    private final AuthorizationScope<T> scope;
    private final String reason;

    Authorization(AuthorizationScope<T> scope, String reason) {
        this.scope = Objects.requireNonNull(scope);
        this.reason = reason;
    }

    public boolean isAllowed() {
        return reason == null;
    }

    public boolean isDenied() {
        return reason != null;
    }

    public AuthorizationScope<T> scope() {
        return scope;
    }

    public Optional<String> reason() {
        return Optional.ofNullable(reason);
    }
}
