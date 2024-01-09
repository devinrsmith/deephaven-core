/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.BlinkTableTools;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ring.RingTableTools;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.kafka.KafkaTools;
import io.deephaven.kafka.KafkaTools.TableType;
import io.deephaven.kafka.KafkaTools.TableType.Append;
import io.deephaven.kafka.KafkaTools.TableType.Blink;
import io.deephaven.kafka.KafkaTools.TableType.Ring;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.util.thread.NamingThreadFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadFactory;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class KafkaTableOptions<K, V> {

    public static <K, V> Builder<K, V> builder() {
        return ImmutableKafkaTableOptions.builder();
    }

    /**
     * The name. Defaults to {@link UUID#randomUUID()}.
     *
     * @return the name
     */
    @Default
    public String name() {
        return UUID.randomUUID().toString();
    }

    /**
     * The Kafka client options.
     *
     * @return the Kafka client options
     */
    public abstract ClientOptions<K, V> clientOptions();

    /**
     * If an opinionated client configuration should be used to extend {@link #clientOptions()}. By default, is
     * {@code true}.
     *
     * <p>
     * The specifics of these may change from release to release. Callers wishing to do their own optimizations are
     * encouraged to set this to {@code false} and finely configure {@link #clientOptions()}. Currently, consists of:
     *
     * <ul>
     * <li>If unset, sets {@link ConsumerConfig#CLIENT_ID_CONFIG} to {@link #name()}.</li>
     * <li>If unset, sets {@link ConsumerConfig#DEFAULT_API_TIMEOUT_MS_CONFIG} to 5 seconds.</li>
     * <li>If unset, sets {@link ConsumerConfig#MAX_POLL_RECORDS_CONFIG} to {@link #chunkSize()}.</li>
     * <li>If unset, sets {@link ConsumerConfig#MAX_PARTITION_FETCH_BYTES_CONFIG} to 16 MiB.</li>
     * <li>If unset, sets {@link ClientOptions#keyDeserializer()} to {@link ByteArrayDeserializer}
     * ({@link ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG} must also be unset).</li>
     * <li>If unset, sets {@link ClientOptions#valueDeserializer()} to {@link ByteArrayDeserializer}
     * ({@link ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG} must also be unset).</li>
     * </ul>
     *
     * @return if opinionated client configuration should be used
     */
    @Default
    public boolean useOpinionatedClientOptions() {
        return true;
    }

    /**
     * The offsets.
     *
     * @return the offsets
     */
    public abstract Offsets offsets();

    /**
     * The record filter. By default, is equivalent to {@code record -> true}, which will include all records.
     *
     * @return the record filter
     */
    @Default
    public Predicate<ConsumerRecord<K, V>> filter() {
        return predicateTrue();
    }

    // todo: give easy way for users to construct w/ specs for specific key / value types

    /**
     * The record processor.
     * 
     * @return
     */
    public abstract ObjectProcessor<ConsumerRecord<K, V>> processor();

    public abstract List<String> columnNames();



    @Default
    public TableType tableType() {
        return TableType.blink();
    }

    /**
     * The extra attributes to set on the resulting table.
     *
     * @return the extra attributes
     */
    public abstract Map<String, Object> extraAttributes();

    /**
     * The update source registrar for the resulting table. By default, is equivalent to
     * {@code ExecutionContext.getContext().getUpdateGraph()}.
     *
     * @return the update source registrar
     */
    @Default
    public UpdateSourceRegistrar updateSourceRegistrar() {
        return ExecutionContext.getContext().getUpdateGraph();
    }

    /**
     * The maximum size of each chunk that will be passed to the {@link StreamConsumer}. Defaults to
     * {@value ArrayBackedColumnSource#BLOCK_SIZE}.
     *
     * @return the chunk size
     */
    @Default
    public int chunkSize() {
        return ArrayBackedColumnSource.BLOCK_SIZE;
    }

    @Default
    public boolean receiveTimestamp() {
        return true;
    }

    public interface Builder<K, V> {
        Builder<K, V> name(String name);

        Builder<K, V> clientOptions(ClientOptions<K, V> clientOptions);

        Builder<K, V> useOpinionatedClientOptions(boolean useOpinionatedClientOptions);

        Builder<K, V> offsets(Offsets offsets);

        Builder<K, V> filter(Predicate<ConsumerRecord<K, V>> filter);

        Builder<K, V> processor(ObjectProcessor<ConsumerRecord<K, V>> processor);

        Builder<K, V> addColumnNames(String element);

        Builder<K, V> addColumnNames(String... elements);

        Builder<K, V> addAllColumnNames(Iterable<String> elements);

        Builder<K, V> tableType(TableType tableType);

        Builder<K, V> putExtraAttributes(String key, Object value);

        Builder<K, V> putExtraAttributes(Map.Entry<String, ?> entry);

        Builder<K, V> putAllExtraAttributes(Map<String, ?> entries);

        Builder<K, V> updateSourceRegistrar(UpdateSourceRegistrar updateSourceRegistrar);

        Builder<K, V> chunkSize(int chunkSize);

        KafkaTableOptions<K, V> build();
    }

    @Check
    final void checkSize() {
        if (processor().size() != columnNames().size()) {
            throw new IllegalArgumentException();
        }
    }

    @Check
    final void checkChunkSize() {
        if (chunkSize() < 1) {
            throw new IllegalArgumentException("chunkSize must be positive");
        }
    }

    @Check
    final void checkAutoCommit() {
        final String enableAutoCommit =
                clientOptions().config().getOrDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        if (!"false".equalsIgnoreCase(enableAutoCommit)) {
            throw new IllegalArgumentException(String.format("Configuration `%s=%s` is unsupported",
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit));
        }
    }

    final Table table() {
        // todo: wrap exec context? liveness?
        return tableType().walk(new TableTypeVisitor());
    }

    final TableDefinition tableDefinition() {
        return receiveTimestamp()
                ? TableDefinition.from(
                        Stream.concat(
                                Stream.of("ReceiveTimestamp"),
                                columnNames().stream())
                                .collect(Collectors.toList()),
                        Stream.concat(
                                Stream.of(Type.instantType()),
                                processor().outputTypes().stream())
                                .collect(Collectors.toList()))
                : TableDefinition.from(columnNames(), processor().outputTypes());
    }

    private class TableTypeVisitor implements TableType.Visitor<Table> {
        @Override
        public Table visit(Blink blink) {
            return adapter().table();
        }

        @Override
        public Table visit(Append append) {
            return BlinkTableTools.blinkToAppendOnly(adapter().table());
        }

        @Override
        public Table visit(Ring ring) {
            return RingTableTools.of(adapter().table(), ring.capacity());
        }
    }

    private StreamToBlinkTableAdapter adapter() {
        final TableDefinition definition = tableDefinition();
        final KafkaPublisherDriver<K, V> publisher = publisher();
        final StreamToBlinkTableAdapter adapter;
        try {
            adapter = new StreamToBlinkTableAdapter(
                    definition,
                    publisher,
                    updateSourceRegistrar(),
                    name(),
                    extraAttributes());
            publisher.start();
        } catch (Throwable t) {
            publisher.errorBeforeStart(t);
            throw t;
        }
        return adapter;
    }

    private KafkaPublisherDriver<K, V> publisher() {
        return KafkaPublisherDriver.of(
                useOpinionatedClientOptions() ? opinionatedClientOptions() : clientOptions(),
                offsets(),
                new KafkaStreamConsumerAdapter<>(filter(), processor(), chunkSize(), receiveTimestamp()),
                threadFactory(),
                callback());
    }

    private ClientOptions<K, V> opinionatedClientOptions() {
        final HashMap<String, String> config = new HashMap<>(clientOptions().config());
        final ClientOptions.Builder<K, V> opinionated = ClientOptions.builder();
        if (!config.containsKey(ConsumerConfig.CLIENT_ID_CONFIG)) {
            config.put(ConsumerConfig.CLIENT_ID_CONFIG, name());
        }
        if (!config.containsKey(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)) {
            // This is local only option, doesn't affect server.
            // This affects the maximum number of records that io.deephaven.kafka.v2.KafkaPublisherDriver.runOnce will
            // receive at once. There's a small tradeoff here; allowing enough for runOnce to do a flush, but also small
            // enough to minimize potential sync wait for StreamPublisher#flush calls (cycle).
            config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(chunkSize()));
        }
        if (!config.containsKey(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG)) {
            // The default of 60 seconds seems high
            config.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Long.toString(Duration.ofSeconds(5).toMillis()));
        }
        if (!config.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
            // Todo: should we be stricter than kafka default?
            // config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.NONE.toString());
        }
        if (!config.containsKey(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG)) {
            // Potential to base as percentage of cycle time? Might be more reasonable to encourage user to set?
        }
        if (!config.containsKey(ConsumerConfig.FETCH_MIN_BYTES_CONFIG)) {
            // Potential to configure for higher throughput; still adheres to max wait ms
        }
        if (!config.containsKey(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG)) {
            // The default of 1 MiB seems too low
            // Overall limit still adheres to FETCH_MAX_BYTES_CONFIG (default 50 MiB)
            config.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, Integer.toString(16 * 1024 * 1024));
        }
        if (clientOptions().keyDeserializer().isPresent()) {
            opinionated.keyDeserializer(clientOptions().keyDeserializer().get());
        } else if (!config.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            // This is not safe; we are assuming the user would have set the deserializer or config directly if they
            // actually cared about the type. We'll get a cast exception at runtime if the user is actually trying to
            // process it as a different type.
            // noinspection unchecked
            opinionated.keyDeserializer((Deserializer<K>) new ByteArrayDeserializer());
        }
        if (clientOptions().valueDeserializer().isPresent()) {
            opinionated.valueDeserializer(clientOptions().valueDeserializer().get());
        } else if (!config.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
            // This is not safe; we are assuming the user would have set the deserializer or config directly if they
            // actually cared about the type. We'll get a cast exception at runtime if the user is actually trying to
            // process it as a different type.
            // noinspection unchecked
            opinionated.valueDeserializer((Deserializer<V>) new ByteArrayDeserializer());
        }
        return opinionated.putAllConfig(config).build();
    }

    // Maybe expose as configuration option in future?
    private static ThreadFactory threadFactory() {
        return new NamingThreadFactory(null, KafkaTableOptions.class, "KafkaPublisherDriver", true);
    }

    // Maybe expose as configuration option in future?
    private static KafkaTools.ConsumerLoopCallback callback() {
        return null;
    }

    private enum PredicateTrue implements Predicate<Object> {
        PREDICATE_TRUE;

        @Override
        public boolean test(Object o) {
            return true;
        }
    }

    private static <T> Predicate<T> predicateTrue() {
        // noinspection unchecked
        return (Predicate<T>) PredicateTrue.PREDICATE_TRUE;
    }
}
