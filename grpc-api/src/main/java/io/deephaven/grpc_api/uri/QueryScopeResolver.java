package io.deephaven.grpc_api.uri;

import io.deephaven.grpc_api.console.GlobalSessionProvider;
import io.deephaven.uri.DeephavenUri;
import io.deephaven.uri.QueryScopeUri;

import java.net.URI;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

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
    public final QueryScopeUri adaptToItem(URI uri) {
        return QueryScopeUri.of(uri);
    }

    @Override
    public final URI adaptToUri(QueryScopeUri item) {
        return item.toURI();
    }

    @Override
    public final Object resolveItem(QueryScopeUri item) {
        return getVariable(item.variableName());
    }

    public final Object getVariable(String variableName) {
        return globalSessionProvider.getGlobalSession().getVariable(variableName);
    }

    @Override
    public final <O> Consumer<O> publishTarget(QueryScopeUri item) {
        return value -> publish(item, value);
    }

    private <O> void publish(QueryScopeUri item, O value) {
        globalSessionProvider.getGlobalSession().setVariable(item.variableName(), value);
    }

    @Override
    public final void forAllItems(BiConsumer<QueryScopeUri, Object> consumer) {
        globalSessionProvider.getGlobalSession().getVariables()
                .forEach((variableName, item) -> consumer.accept(QueryScopeUri.of(variableName), item));
    }
}
