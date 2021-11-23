package io.deephaven.grpc_api.uri;

import io.deephaven.grpc_api.console.GlobalSessionProvider;
import io.deephaven.uri.QueryScopeUri;
import io.deephaven.util.auth.AuthContext;

import javax.inject.Inject;

public final class QueryScopeResolverOpen extends QueryScopeResolver {

    @Inject
    public QueryScopeResolverOpen(GlobalSessionProvider globalSessionProvider) {
        super(globalSessionProvider);
    }

    @Override
    public Authorization<QueryScopeUri> authorization(AuthorizationScope<QueryScopeUri> scope, AuthContext context) {
        if (scope.isGlobal()) {
            return Authorization.allow(scope);
        }
        if (scope.path().orElseThrow().variableName().startsWith("__")) {
            return Authorization.deny(scope,
                    "The query scope resolver does not resolve private names (starts with '__').");
        }
        return Authorization.allow(scope);
    }
}
