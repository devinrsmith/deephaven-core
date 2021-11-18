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
    public boolean isEnabled(AuthContext auth) {
        return true;
    }

    @Override
    public boolean isEnabled(AuthContext auth, QueryScopeUri item) {
        return !item.variableName().startsWith("__");
    }

    @Override
    public String helpEnable(AuthContext auth) {
        throw new IllegalStateException();
    }

    @Override
    public String helpEnable(AuthContext auth, QueryScopeUri item) {
        return "Unable to resolve variables that start with '__'.";
    }
}
