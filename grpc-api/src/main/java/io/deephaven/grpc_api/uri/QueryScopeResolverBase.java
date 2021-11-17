package io.deephaven.grpc_api.uri;

import io.deephaven.grpc_api.console.GlobalSessionProvider;
import io.deephaven.uri.DeephavenUri;
import io.deephaven.uri.QueryScopeUri;

import java.net.URI;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public abstract class QueryScopeResolverBase extends UriResolverDeephaven<QueryScopeUri> {
    public static QueryScopeResolverBase get() {
        return UriRouterInstance.get().find(QueryScopeResolverBase.class).get();
    }

    private final GlobalSessionProvider globalSessionProvider;

    public QueryScopeResolverBase(GlobalSessionProvider globalSessionProvider) {
        this.globalSessionProvider = Objects.requireNonNull(globalSessionProvider);
    }

    @Override
    public final Set<String> schemes() {
        return Collections.singleton(DeephavenUri.LOCAL_SCHEME);
    }

    @Override
    public final boolean isResolvable(URI uri) {
        return QueryScopeUri.isWellFormed(uri);
    }

    @Override
    public final QueryScopeUri adapt(URI uri) {
        return QueryScopeUri.of(uri);
    }

    @Override
    public final Object resolve(QueryScopeUri uri) throws InterruptedException {
        return getVariable(uri.variableName());
    }

    public final Object getVariable(String variableName) {
        return globalSessionProvider.getGlobalSession().getVariable(variableName);
    }
}
