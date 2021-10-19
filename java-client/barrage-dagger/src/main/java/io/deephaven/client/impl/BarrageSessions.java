package io.deephaven.client.impl;

import io.deephaven.DeephavenUriI;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.db.tables.Table;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.qst.table.TicketTable;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class BarrageSessions {

    public static BarrageSessions tmp() {
        return new BarrageSessions(
                Executors.newScheduledThreadPool(4),
                new RootAllocator(),
                new ConcurrentHashMap<>(),
                BarrageLocalResolverInstance.get());
    }

    private final ScheduledExecutorService executor;

    private final BufferAllocator allocator;

    private final Map<String, BarrageSession> sessions;

    private final BarrageLocalResolver resolver;

    private BarrageSessions(ScheduledExecutorService executor, BufferAllocator allocator,
            Map<String, BarrageSession> sessions, BarrageLocalResolver resolver) {
        this.executor = Objects.requireNonNull(executor);
        this.allocator = Objects.requireNonNull(allocator);
        this.sessions = Objects.requireNonNull(sessions);
        this.resolver = Objects.requireNonNull(resolver);
    }

    private BarrageSession newSession(ManagedChannel channel) {
        return DaggerDeephavenBarrageRoot.create().factoryBuilder()
                .allocator(allocator)
                .managedChannel(channel)
                .scheduler(executor)
                .build()
                .newBarrageSession();
    }

    public BarrageSession session(String target) {
        return sessions.computeIfAbsent(target, this::newSession);
    }

    public Table subscribe(String deephavenUri) throws TableHandleException, InterruptedException {
        return subscribe(DeephavenUriI.from(deephavenUri));
    }

    public Table subscribe(DeephavenUriI uri) throws TableHandleException, InterruptedException {
        return subscribe(uri, BarrageSubscriptionOptions.builder().useDeephavenNulls(true).build());
    }

    public Table subscribe(DeephavenUriI uri, BarrageSubscriptionOptions options)
            throws TableHandleException, InterruptedException {
        if (!uri.host().isPresent()) {
            return DeephavenUriLocal.of(resolver, uri);
        }
        final TicketTable ticketTable = DeephavenUriBarrage.of(uri);
        final String target = resolveTarget(uri);
        final BarrageSubscription sub = session(target).subscribe(ticketTable, options);
        return sub.entireTable();
    }

    private BarrageSession newSession(String target) {
        return newSession(ManagedChannelBuilder.forTarget(target).usePlaintext().build());
    }

    private static String resolveTarget(DeephavenUriI uri) {
        // TODO: DNS SRV
        return uri.host().get() + ":" + uri.port().orElse(8080);
    }
}
