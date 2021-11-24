package io.deephaven.grpc_api.uri;

import io.deephaven.client.impl.BarrageSession;
import io.deephaven.client.impl.BarrageSessionFactoryBuilder;
import io.deephaven.client.impl.BarrageSubscription;
import io.deephaven.client.impl.ChannelHelper;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.db.tables.Table;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.uri.DeephavenTarget;
import io.deephaven.uri.DeephavenUri;
import io.deephaven.uri.RemoteUri;
import io.deephaven.uri.RemoteUriAdapter;
import io.deephaven.uri.StructuredUri;
import io.grpc.ManagedChannel;
import org.apache.arrow.memory.BufferAllocator;

import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class BarrageTableResolver extends UriResolverBase<RemoteUri> {

    public static BarrageTableResolver get() {
        return UriRouterInstance.get().find(BarrageTableResolver.class).get();
    }

    public static final BarrageSubscriptionOptions OPTIONS = BarrageSubscriptionOptions.builder()
            .useDeephavenNulls(true)
            .build();

    private final BarrageSessionFactoryBuilder builder;

    private final ScheduledExecutorService executor;

    private final BufferAllocator allocator;

    private final Map<DeephavenTarget, BarrageSession> sessions;

    public BarrageTableResolver(BarrageSessionFactoryBuilder builder, ScheduledExecutorService executor,
            BufferAllocator allocator) {
        this.builder = Objects.requireNonNull(builder);
        this.executor = Objects.requireNonNull(executor);
        this.allocator = Objects.requireNonNull(allocator);
        this.sessions = new ConcurrentHashMap<>();
    }

    @Override
    public final Set<String> schemes() {
        return Stream.of(DeephavenUri.PLAINTEXT_SCHEME, DeephavenUri.SECURE_SCHEME).collect(Collectors.toSet());
    }

    @Override
    public final boolean isResolvable(URI uri) {
        return RemoteUri.isWellFormed(uri);
    }

    @Override
    public final RemoteUri adaptToPath(URI uri) {
        return RemoteUri.of(uri);
    }

    @Override
    public final URI adaptToUri(RemoteUri path) {
        return path.toURI();
    }

    @Override
    public final Object resolvePath(RemoteUri path) throws InterruptedException {
        try {
            return subscribe(path);
        } catch (TableHandleException e) {
            throw e.asUnchecked();
        }
    }

    @Override
    public void forAllPaths(BiConsumer<RemoteUri, Object> consumer) {

    }

    @Override
    public void forPaths(Predicate<RemoteUri> predicate, BiConsumer<RemoteUri, Object> consumer) {

    }

    /**
     * Create a full-subscription to the remote URI. Uses {@link #OPTIONS}.
     *
     * @param remoteUri the remote URI
     * @return the subscribed table
     */
    public final Table subscribe(RemoteUri remoteUri) throws InterruptedException, TableHandleException {
        final DeephavenTarget target = remoteUri.target();
        final TableSpec table = RemoteUriAdapter.of(remoteUri);
        return subscribe(target, table, OPTIONS);
    }

    /**
     * Create a full-subscription to the {@code table} via the {@code targetUri}. Uses {@link #OPTIONS}.
     *
     * @param targetUri the target URI
     * @param table the table spec
     * @return the subscribed table
     */
    public final Table subscribe(String targetUri, TableSpec table) throws TableHandleException, InterruptedException {
        return subscribe(DeephavenTarget.of(URI.create(targetUri)), table, OPTIONS);
    }

    /**
     * Create a full-subscription to the {@code table} via the {@code target}.
     *
     * @param target the target
     * @param table the table
     * @param options the options
     * @return the subscribed table
     */
    public final Table subscribe(DeephavenTarget target, TableSpec table, BarrageSubscriptionOptions options)
            throws TableHandleException, InterruptedException {
        final BarrageSession session = session(target);
        final BarrageSubscription sub = session.subscribe(table, options);
        return sub.entireTable();
    }

    private BarrageSession session(DeephavenTarget target) {
        // TODO (deephaven-core#1482): BarrageTableResolver cleanup
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
}
