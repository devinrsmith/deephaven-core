package io.deephaven.grpc_api.uri;

import io.deephaven.grpc_api.appmode.ApplicationStates;
import io.deephaven.uri.ApplicationUri;
import io.deephaven.util.auth.AuthContext;

import javax.inject.Inject;

public final class ApplicationResolverSuperUser extends ApplicationResolver {
    @Inject
    public ApplicationResolverSuperUser(ApplicationStates states) {
        super(states);
    }

    @Override
    public Authorization<ApplicationUri> authorization(AuthorizationScope<ApplicationUri> scope, AuthContext context) {
        if (scope.isWrite()) {
            return Authorization.deny(scope, "The application resolver does not allow publishing");
        }
        if (context == null || !context.isSuperUser()) {
            return Authorization.deny(scope, "The application resolver requires a super-user");
        }
        return Authorization.allow(scope);
    }
}
