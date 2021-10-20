package io.deephaven.client.impl;

import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.db.tables.Table;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.uri.DeephavenTarget;
import io.deephaven.uri.LocalApplicationUri;
import io.deephaven.uri.LocalUri;
import io.deephaven.uri.LocalFieldUri;
import io.deephaven.uri.LocalQueryScopeUri;
import io.deephaven.uri.RemoteUri;
import io.deephaven.uri.ResolvableUri;
import io.deephaven.uri.ResolvableUri.Visitor;
import io.deephaven.uri.TableResolver;
import io.grpc.ManagedChannel;
import org.apache.arrow.memory.BufferAllocator;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.URI;
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
    public boolean canResolve(ResolvableUri uri) {
        return uri.walk(new CanResolve()).out();
    }

    @Override
    public Table resolve(ResolvableUri uri) throws InterruptedException {
        try {
            return subscribe(Resolver.of(uri));
        } catch (TableHandleException e) {
            throw e.asUnchecked();
        }
    }

    public Table subscribe(RemoteUri uri) throws TableHandleException, InterruptedException {
        return subscribe(uri, OPTIONS);
    }

    public Table subscribe(RemoteUri uri, BarrageSubscriptionOptions options)
            throws TableHandleException, InterruptedException {
        final DeephavenTarget target = uri.target();
        final TableSpec spec = RemoteResolver.of(uri);
        final BarrageSubscription sub = session(target).subscribe(spec, options);
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

    static class CanResolve implements ResolvableUri.Visitor {
        private Boolean out;

        public boolean out() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(LocalUri localUri) {
            out = false;
        }

        @Override
        public void visit(RemoteUri remoteUri) {
            remoteUri.uri().walk(new Visitor() {
                @Override
                public void visit(LocalUri localUri) {
                    localUri.walk(new LocalUri.Visitor() {
                        @Override
                        public void visit(LocalFieldUri fieldUri) {
                            out = true;
                        }

                        @Override
                        public void visit(LocalApplicationUri applicationField) {
                            out = true;
                        }

                        @Override
                        public void visit(LocalQueryScopeUri queryScope) {
                            out = true;
                        }
                    });
                }

                @Override
                public void visit(RemoteUri remoteUri) {
                    out = false; // don't support proxy yet
                }

                @Override
                public void visit(URI uri) {
                    out = false; // don't support generic URI yet
                }
            });
        }

        @Override
        public void visit(URI uri) {
            out = false;
        }
    }

    static class Resolver implements ResolvableUri.Visitor {

        public static RemoteUri of(ResolvableUri uri) {
            return uri.walk(new Resolver()).out();
        }

        private RemoteUri out;

        public RemoteUri out() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(LocalUri localUri) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void visit(RemoteUri remoteUri) {
            out = remoteUri;
        }

        @Override
        public void visit(URI uri) {
            throw new UnsupportedOperationException();
        }
    }

    static class RemoteResolver implements ResolvableUri.Visitor {

        public static TableSpec of(RemoteUri remoteUri) {
            return remoteUri.uri().walk(new RemoteResolver(remoteUri.target())).out();
        }

        private final DeephavenTarget target;
        private TableSpec out;

        public RemoteResolver(DeephavenTarget target) {
            this.target = Objects.requireNonNull(target);
        }

        public TableSpec out() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(LocalUri localUri) {
            localUri.walk(new LocalUri.Visitor() {
                @Override
                public void visit(LocalFieldUri fieldUri) {
                    out = TicketTable.fromApplicationField(target.host(), fieldUri.fieldName());
                }

                @Override
                public void visit(LocalApplicationUri applicationField) {
                    out = TicketTable.fromApplicationField(applicationField.applicationId(),
                            applicationField.fieldName());
                }

                @Override
                public void visit(LocalQueryScopeUri queryScope) {
                    out = TicketTable.fromQueryScopeField(queryScope.variableName());
                }
            });
        }

        @Override
        public void visit(RemoteUri remoteUri) {
            throw new UnsupportedOperationException("Proxying not supported yet");
        }

        @Override
        public void visit(URI uri) {
            throw new UnsupportedOperationException("Remote generic URIs not supported yet");
        }
    }
}
