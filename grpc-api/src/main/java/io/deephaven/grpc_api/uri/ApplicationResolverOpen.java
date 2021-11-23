package io.deephaven.grpc_api.uri;

import io.deephaven.grpc_api.appmode.ApplicationStates;
import io.deephaven.uri.ApplicationUri;
import io.deephaven.util.auth.AuthContext;

import javax.inject.Inject;

public final class ApplicationResolverOpen extends ApplicationResolver {
    @Inject
    public ApplicationResolverOpen(ApplicationStates states) {
        super(states);
    }

    @Override
    public Authorization<ApplicationUri> authorization(AuthorizationScope<ApplicationUri> scope, AuthContext context) {
        if (scope.isWrite()) {
            return Authorization.deny(scope, "The application resolver does not allow publishing");
        }
        if (scope.isGlobal()) {
            return Authorization.allow(scope);
        }
        if (scope.path().orElseThrow().fieldName().startsWith("__")) {
            return Authorization.deny(scope,
                    "The application resolver does not resolve private names (starts with '__').");
        }
        return Authorization.allow(scope);
    }
}
