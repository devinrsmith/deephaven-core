package io.deephaven.client.impl;

import io.deephaven.DeephavenUri;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableSpec;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class BarrageTools {

    private static final ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);

    private static final BufferAllocator allocator = new RootAllocator();

    private static final Map<String, BarrageSession> sessions = new ConcurrentHashMap<>();

    public static BarrageSession newSession(BufferAllocator bufferAllocator, ManagedChannel channel,
            ScheduledExecutorService scheduler) {
        return DaggerDeephavenBarrageRoot.create().factoryBuilder()
                .allocator(bufferAllocator)
                .managedChannel(channel)
                .scheduler(scheduler)
                .build()
                .newBarrageSession();
    }

    public static BarrageSession session(String target) {
        return sessions.computeIfAbsent(target,
                s -> newSession(allocator, ManagedChannelBuilder.forTarget(target).usePlaintext().build(), executor));
    }

    public static Table subscribe(String target, TableHeader header, TableSpec table)
            throws TableHandleException, InterruptedException {
        final BarrageSubscriptionOptions options = BarrageSubscriptionOptions.builder()
                .useDeephavenNulls(true)
                .build();
        final BarrageSubscription sub = session(target).subscribe(TableDefinition.from(header), table, options);
        return sub.entireTable();
    }

    public static Table subscribe(String target, TableSpec table) throws TableHandleException, InterruptedException {
        final BarrageSubscriptionOptions options = BarrageSubscriptionOptions.builder()
                .useDeephavenNulls(true)
                .build();
        final BarrageSubscription sub = session(target).subscribe(table, options);
        return sub.entireTable();
    }

    public static Table subscribe(String deephavenUri) throws TableHandleException, InterruptedException {
        return subscribe(DeephavenUri.of(deephavenUri));
    }

    private static Table subscribe(DeephavenUri uri) throws TableHandleException, InterruptedException {
        if (uri.isLocal()) {
            throw new UnsupportedOperationException("Local is not supported yet");
        }
        final String target = uri.host().get() + ":" + uri.port().orElse(10000);
        return subscribe(target, uri.ticket());
    }
}
