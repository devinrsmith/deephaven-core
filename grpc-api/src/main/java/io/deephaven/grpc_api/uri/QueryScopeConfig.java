package io.deephaven.grpc_api.uri;

import io.deephaven.grpc_api.console.GlobalSessionProvider;
import io.deephaven.uri.QueryScopeUri;
import io.deephaven.util.auth.AuthContext;

import java.util.Objects;

public final class QueryScopeConfig implements UriResolverDeephavenBase.Config<QueryScopeUri> {
    private final GlobalSessionProvider globalSessionProvider;

    public QueryScopeConfig(GlobalSessionProvider globalSessionProvider) {
        this.globalSessionProvider = Objects.requireNonNull(globalSessionProvider);
    }

    @Override
    public boolean isEnabled(AuthContext auth) {
        return false;
    }

    @Override
    public boolean isEnabled(AuthContext auth, QueryScopeUri uri) {
        return false;
    }

    @Override
    public String helpEnable(AuthContext auth) {
        return null;
    }

    @Override
    public String helpEnable(AuthContext auth, QueryScopeUri uri) {
        return null;
    }

    @Override
    public Object resolve(AuthContext auth, QueryScopeUri uri) {
        return globalSessionProvider.getGlobalSession().getVariable(uri.variableName());
    }
}
