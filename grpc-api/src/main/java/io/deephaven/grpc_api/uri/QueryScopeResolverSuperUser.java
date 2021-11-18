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
    public boolean isEnabled(AuthContext auth) {
        return auth != null && auth.isSuperUser();
    }

    @Override
    public boolean isEnabled(AuthContext auth, QueryScopeUri item) {
        return true;
    }

    @Override
    public String helpEnable(AuthContext auth) {
        return "Enabled for super-users.";
    }

    @Override
    public String helpEnable(AuthContext auth, QueryScopeUri item) {
        throw new IllegalStateException();
    }
}
