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

    /**
     * {@code true} if {@link AuthContext#isSuperUser()}, otherwise looks up the property key
     * {@link #BARRAGE_TABLE_RESOLVER_ENABLED_KEY}, {@code true} when equal to {@value #TRUE}; {@code false} otherwise.
     *
     * @return {@code true} if URI resolvers is enabled, {@code false} by default
     */
    @Override
    public boolean isEnabled(AuthContext auth) {
        return auth.isSuperUser() || TRUE.equals(System.getProperty(BARRAGE_TABLE_RESOLVER_ENABLED_KEY, FALSE));
    }

    @Override
    public boolean isEnabled(AuthContext auth, RemoteUri item) {
        return true;
    }

    @Override
    public String helpEnable(AuthContext auth) {
        return String.format("Enabled for super-users. To enable for all, set system property '%s' to 'true'.",
                BARRAGE_TABLE_RESOLVER_ENABLED_KEY);
    }

    @Override
    public String helpEnable(AuthContext auth, RemoteUri item) {
        throw new IllegalStateException();
    }
}
