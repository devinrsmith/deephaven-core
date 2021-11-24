package io.deephaven.grpc_api.uri;

import io.deephaven.client.impl.BarrageSessionFactoryBuilder;
import io.deephaven.uri.DeephavenTarget;
import io.deephaven.uri.RemoteUri;
import io.deephaven.util.auth.AuthContext;
import org.apache.arrow.memory.BufferAllocator;

import javax.inject.Inject;
import java.net.URI;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

public final class BarrageTableResolverList extends BarrageTableResolver {

    private final Set<RemoteUri> allowed;

    @Inject
    public BarrageTableResolverList(BarrageSessionFactoryBuilder builder, ScheduledExecutorService executor,
                                    BufferAllocator allocator) {
        super(builder, executor, allocator);
        this.allowed = Collections.singleton(RemoteUri.of(URI.create("dh+plain://w2/scope/currentTime")));
    }

    @Override
    public Authorization<RemoteUri> authorization(AuthorizationScope<RemoteUri> scope, AuthContext context) {
        if (scope.isWrite()) {
            // todo: you could potentially proxy writes
            return Authorization.deny(scope, "The barrage resolver does not allow publishing");
        }
        if (scope.isGlobal()) {
            return Authorization.allow(scope);
        }
        final RemoteUri uri = scope.path().orElseThrow();
        if (allowed.contains(uri)) {
            return Authorization.allow(scope);
        }
        return Authorization.deny(scope, String.format("The barrage resolver does not have '%s' in the allowed list", uri));
    }

    @Override
    public void forAllPaths(BiConsumer<RemoteUri, Object> consumer) {
        for (RemoteUri remoteUri : allowed) {
            try {
                consumer.accept(remoteUri, resolvePath(remoteUri));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @Override
    public void forPaths(Predicate<RemoteUri> predicate, BiConsumer<RemoteUri, Object> consumer) {
        for (RemoteUri remoteUri : allowed) {
            if (!predicate.test(remoteUri)) {
                continue;
            }
            try {
                consumer.accept(remoteUri, resolvePath(remoteUri));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
