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
import java.util.function.Predicate;

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
    public final QueryScopeUri adaptToPath(URI uri) {
        return QueryScopeUri.of(uri);
    }

    @Override
    public final URI adaptToUri(QueryScopeUri path) {
        return path.toURI();
    }

    @Override
    public final Object resolvePath(QueryScopeUri path) {
        return getVariable(path.variableName());
    }

    public final Object getVariable(String variableName) {
        return globalSessionProvider.getGlobalSession().getVariable(variableName);
    }

    @Override
    public final <O> Consumer<O> publishTarget(QueryScopeUri path) {
        return value -> publish(path, value);
    }

    private <O> void publish(QueryScopeUri item, O value) {
        globalSessionProvider.getGlobalSession().setVariable(item.variableName(), value);
    }

    @Override
    public final void forAllPaths(BiConsumer<QueryScopeUri, Object> consumer) {
        globalSessionProvider.getGlobalSession().getVariables().forEach(new Adapter(null, consumer));
    }

    @Override
    public final void forPaths(Predicate<QueryScopeUri> predicate, BiConsumer<QueryScopeUri, Object> consumer) {
        globalSessionProvider.getGlobalSession().getVariables().forEach(new Adapter(Objects.requireNonNull(predicate), consumer));
    }

    private static class Adapter implements BiConsumer<String, Object> {
        private final Predicate<QueryScopeUri> predicate;
        private final BiConsumer<QueryScopeUri, Object> delegate;

        Adapter(Predicate<QueryScopeUri> predicate, BiConsumer<QueryScopeUri, Object> delegate) {
            this.predicate = predicate;
            this.delegate = Objects.requireNonNull(delegate);
        }

        @Override
        public void accept(String variableName, Object value) {
            final QueryScopeUri uri = QueryScopeUri.of(variableName);
            if (predicate == null || predicate.test(uri)) {
                delegate.accept(uri, value);
            }
        }
    }
}
