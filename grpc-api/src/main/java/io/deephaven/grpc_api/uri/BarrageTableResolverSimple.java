package io.deephaven.grpc_api.uri;

import io.deephaven.client.impl.BarrageSessionFactoryBuilder;
import io.deephaven.uri.RemoteUri;
import io.deephaven.util.auth.AuthContext;
import org.apache.arrow.memory.BufferAllocator;

import javax.inject.Inject;
import java.util.concurrent.ScheduledExecutorService;

public final class BarrageTableResolverSimple extends BarrageTableResolver {

    public static final String BARRAGE_TABLE_RESOLVER_ENABLED_KEY =
            UriRouterPropertyConfig.propertyKey(BarrageTableResolver.class);

    /**
     * The {@code true} value.
     */
    public static final String TRUE = "true";

    /**
     * The {@code false} value.
     */
    public static final String FALSE = "false";

    @Inject
    public BarrageTableResolverSimple(BarrageSessionFactoryBuilder builder, ScheduledExecutorService executor,
            BufferAllocator allocator) {
        super(builder, executor, allocator);
    }

    @Override
    public Authorization<RemoteUri> authorization(AuthScope<RemoteUri> scope, AuthContext context) {
        if (scope.isWrite()) {
            return Authorization.deny(scope, "The barrage resolver does not allow publishing");
        }
        if (context != null && context.isSuperUser()) {
            return Authorization.allow(scope);
        }
        if (TRUE.equals(System.getProperty(BARRAGE_TABLE_RESOLVER_ENABLED_KEY, FALSE))) {
            return Authorization.allow(scope);
        }
        return Authorization.deny(scope, String.format(
                "The barrage resolver is enabled for super-users. To enable for all, set system property '%s' to 'true'.",
                BARRAGE_TABLE_RESOLVER_ENABLED_KEY));
    }
}
