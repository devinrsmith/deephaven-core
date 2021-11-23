package io.deephaven.grpc_api.uri;

import io.deephaven.client.impl.BarrageSessionFactoryBuilder;
import io.deephaven.uri.RemoteUri;
import io.deephaven.util.auth.AuthContext;
import org.apache.arrow.memory.BufferAllocator;

import javax.inject.Inject;
import java.util.concurrent.ScheduledExecutorService;

public final class BarrageTableResolverSuperUser extends BarrageTableResolver {

    @Inject
    public BarrageTableResolverSuperUser(BarrageSessionFactoryBuilder builder, ScheduledExecutorService executor,
            BufferAllocator allocator) {
        super(builder, executor, allocator);
    }

    @Override
    public Authorization<RemoteUri> authorization(AuthorizationScope<RemoteUri> scope, AuthContext context) {
        if (scope.isWrite()) {
            return Authorization.deny(scope, "The barrage resolver does not allow publishing");
        }
        if (context == null || !context.isSuperUser()) {
            return Authorization.deny(scope, "The barrage resolver requires a super-user");
        }
        return Authorization.allow(scope);
    }
}
