package io.deephaven.grpc_api.uri;

import io.deephaven.grpc_api.console.GlobalSessionProvider;
import io.deephaven.uri.DeephavenUri;
import io.deephaven.uri.QueryScopeUri;

import java.net.URI;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public abstract class QueryScopeResolver extends UriResolverBase<QueryScopeUri> {
    public static QueryScopeResolver get() {
        return UriRouterInstance.get().find(QueryScopeResolver.class).get();
    }

    private final GlobalSessionProvider globalSessionProvider;

    public QueryScopeResolver(GlobalSessionProvider globalSessionProvider) {
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
    public final Object resolveItem(QueryScopeUri item) throws InterruptedException {
        return getVariable(item.variableName());
    }

    public final Object getVariable(String variableName) {
        return globalSessionProvider.getGlobalSession().getVariable(variableName);
    }
}
