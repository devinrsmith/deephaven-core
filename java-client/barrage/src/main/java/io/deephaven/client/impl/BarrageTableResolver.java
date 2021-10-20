package io.deephaven.client.impl;

import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.db.tables.Table;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.uri.DeephavenTarget;
import io.deephaven.uri.DeephavenUri;
import io.deephaven.uri.DeephavenUriApplicationField;
import io.deephaven.uri.DeephavenUriField;
import io.deephaven.uri.DeephavenUriProxy;
import io.deephaven.uri.DeephavenUriQueryScope;
import io.deephaven.uri.TableResolver;
import io.grpc.ManagedChannel;
import org.apache.arrow.memory.BufferAllocator;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

@Singleton
public class BarrageTableResolver implements TableResolver {

    private static final BarrageSubscriptionOptions OPTIONS = BarrageSubscriptionOptions.builder()
            .useDeephavenNulls(true)
            .build();

    private final BarrageSessionFactoryBuilder builder;

    private final ScheduledExecutorService executor;

    private final BufferAllocator allocator;

    private final Map<DeephavenTarget, BarrageSession> sessions;

    @Inject
    public BarrageTableResolver(
            BarrageSessionFactoryBuilder builder, ScheduledExecutorService executor, BufferAllocator allocator) {
        this.builder = Objects.requireNonNull(builder);
        this.executor = Objects.requireNonNull(executor);
        this.allocator = Objects.requireNonNull(allocator);
        this.sessions = new ConcurrentHashMap<>();
    }

    @Override
    public boolean canResolve(DeephavenUri uri) {
        return uri.isRemote();
    }

    @Override
    public Table resolve(DeephavenUri uri) throws InterruptedException {
        try {
            return subscribe(uri);
        } catch (TableHandleException e) {
            throw e.asUnchecked();
        }
    }

    public Table subscribe(DeephavenUri uri) throws TableHandleException, InterruptedException {
        return subscribe(uri, OPTIONS);
    }

    public Table subscribe(DeephavenUri uri, BarrageSubscriptionOptions options)
            throws TableHandleException, InterruptedException {
        if (!uri.isRemote()) {
            throw new IllegalArgumentException("Can only subscribe to Deephaven remote URIs with Barrage");
        }
        final DeephavenTarget target = uri.target().orElseThrow(IllegalStateException::new);
        final TableSpec tableSpec = TableSpecAdapter.of(uri);
        final BarrageSubscription sub = session(target).subscribe(tableSpec, options);
        return sub.entireTable();
    }

    private BarrageSession session(DeephavenTarget target) {
        // TODO: cleanup sessions after all tables are gone
        return sessions.computeIfAbsent(target, this::newSession);
    }

    private BarrageSession newSession(DeephavenTarget target) {
        return newSession(ChannelHelper.channel(target));
    }

    private BarrageSession newSession(ManagedChannel channel) {
        return builder
                .allocator(allocator)
                .managedChannel(channel)
                .scheduler(executor)
                .build()
                .newBarrageSession();
    }

    public static class TableSpecAdapter implements DeephavenUri.Visitor {

        public static TableSpec of(DeephavenUri uri) {
            return uri.walk(new TableSpecAdapter()).out();
        }

        private TableSpec out;

        private TableSpecAdapter() {}

        public TableSpec out() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(DeephavenUriField field) {
            out = TicketTable.fromApplicationField(field.applicationId(), field.fieldName());
        }

        @Override
        public void visit(DeephavenUriApplicationField applicationField) {
            out = TicketTable.fromApplicationField(applicationField.applicationId(), applicationField.fieldName());
        }

        @Override
        public void visit(DeephavenUriQueryScope queryScope) {
            out = TicketTable.fromQueryScopeField(queryScope.variableName());
        }

        @Override
        public void visit(DeephavenUriProxy proxy) {
            // out = TicketTable.of(proxy.path().toString());
            throw new UnsupportedOperationException("Proxy not supported yet");
        }
    }
}
