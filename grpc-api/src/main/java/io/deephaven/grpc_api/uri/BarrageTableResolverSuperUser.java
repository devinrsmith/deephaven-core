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
    public boolean isEnabled(AuthContext auth) {
        return auth != null && auth.isSuperUser();
    }

    @Override
    public boolean isEnabled(AuthContext auth, RemoteUri item) {
        return true;
    }

    @Override
    public String helpEnable(AuthContext auth) {
        return "Enabled for super-users.";
    }

    @Override
    public String helpEnable(AuthContext auth, RemoteUri item) {
        throw new IllegalStateException();
    }
}
