package io.deephaven.grpc_api.uri;

import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.grpc_api.console.GlobalSessionProvider;
import io.deephaven.uri.DeephavenUri;
import io.deephaven.uri.QueryScopeUri;
import io.deephaven.uri.RemoteUri;
import io.deephaven.util.auth.AuthContext;

import javax.inject.Inject;
import java.net.URI;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * The query scope table resolver is able to resolve {@link QueryScopeUri query scope URIs}.
 *
 * <p>
 * For example, {@code dh:///scope/my_table}.
 *
 * @see QueryScopeUri query scope URI format
 */
public final class QueryScopeResolver implements UriResolver {

    public static QueryScopeResolver get() {
        return UriRouterInstance.get().find(QueryScopeResolver.class).get();
    }

    private final GlobalSessionProvider globalSessionProvider;

    @Inject
    public QueryScopeResolver(GlobalSessionProvider globalSessionProvider) {
        this.globalSessionProvider = Objects.requireNonNull(globalSessionProvider);
    }

    @Override
    public Set<String> schemes() {
        return Collections.singleton(DeephavenUri.LOCAL_SCHEME);
    }

    @Override
    public boolean isResolvable(URI uri) {
        return QueryScopeUri.isWellFormed(uri);
    }

    @Override
    public Object resolve(URI uri) {
        return resolve(QueryScopeUri.of(uri));
    }

    @Override
    public Object resolveSafely(AuthContext auth, URI uri) throws InterruptedException {
        if (!config.isEnabled(auth)) {
            throw new UnsupportedOperationException(
                    String.format("Query scope resolver is disabled. %s", config.helpEnable(auth)));
        }
        final QueryScopeUri queryScopeUri = QueryScopeUri.of(uri);
        if (!config.isEnabled(auth, remoteUri)) {
            throw new UnsupportedOperationException(String.format("Barrage table resolver is disable for URI '%s'. %s",
                    uri, config.helpEnable(auth, remoteUri)));
        }
        try {
            return subscribe(remoteUri);
        } catch (TableHandleException e) {
            throw e.asUnchecked();
        }
    }

    public Object resolve(QueryScopeUri uri) {
        return resolve(uri.variableName());
    }

    public Object resolve(String variableName) {
        return globalSessionProvider.getGlobalSession().getVariable(variableName, null);
    }
}
