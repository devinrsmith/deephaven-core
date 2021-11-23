package io.deephaven.grpc_api.uri;

import io.deephaven.grpc_api.console.GlobalSessionProvider;
import io.deephaven.uri.QueryScopeUri;
import io.deephaven.util.auth.AuthContext;

import javax.inject.Inject;

public final class QueryScopeResolverSuperUser extends QueryScopeResolver {

    @Inject
    public QueryScopeResolverSuperUser(GlobalSessionProvider globalSessionProvider) {
        super(globalSessionProvider);
    }

    @Override
    public Authorization<QueryScopeUri> authorization(AuthorizationScope<QueryScopeUri> scope, AuthContext context) {
        if (scope.isWrite()) {
            return Authorization.deny(scope, "The query scope resolver does not allow publishing");
        }
        if (context == null || !context.isSuperUser()) {
            return Authorization.deny(scope, "The query scope resolver requires a super-user");
        }
        return Authorization.allow(scope);
    }
}
