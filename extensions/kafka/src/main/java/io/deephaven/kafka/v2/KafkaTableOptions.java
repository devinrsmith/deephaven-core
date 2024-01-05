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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadFactory;
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
     * If opinionated client configuration should be used to extend {@link #clientOptions()}. These extensions may
     * change from release to release. Callers wishing to do their own optimizations are encouraged to set this to
     * {@code false} and configure {@link #clientOptions()} explicitly.
     *
     * <ul>
     * <li>Sets {@link ConsumerConfig#CLIENT_ID_CONFIG} to {@link #name()} if unset.</li>
     * <li>Sets {@link ConsumerConfig#MAX_POLL_RECORDS_CONFIG} to {@link #chunkSize()} if unset.</li>
     * <li>Sets {@link ConsumerConfig#MAX_PARTITION_FETCH_BYTES_CONFIG} to 16MiB if unset.</li>
     * <li>Sets {@link ClientOptions#keyDeserializer()} to {@link ByteArrayDeserializer} if unset and
     * {@link ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG} is unset.</li>
     * <li>Sets {@link ClientOptions#valueDeserializer()} to {@link ByteArrayDeserializer} if unset and
     * {@link ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG} is unset.</li>
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

    // todo: give easy way for users to construct w/ specs for specific key / value types
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
     * The update source registrar for the resulting table.
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

    final StreamToBlinkTableAdapter adapter() {
        final TableDefinition definition = tableDefinition();
        final KafkaPublisherDriver<K, V> publisher = KafkaPublisherDriver.of(
                useOpinionatedClientOptions() ? opinionatedClientOptions() : clientOptions(),
                offsets(),
                new KafkaStreamConsumerAdapter<>(processor(), chunkSize(), receiveTimestamp()),
                null);
        try {
            final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(
                    definition,
                    publisher,
                    updateSourceRegistrar(),
                    name(),
                    extraAttributes());
            publisher.start(threadFactory());
            return adapter;
        } catch (Throwable t) {
            publisher.startError(t);
            throw t;
        }
    }

    final Table table() {
        // todo: wrap exec context? liveness?
        return tableType().walk(new TableTypeVisitor());
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

    private TableDefinition tableDefinition() {
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

    private ClientOptions<K, V> opinionatedClientOptions() {
        // todo enable.auto.commit=false
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
        if (!config.containsKey(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG)) {
            // Potential to base as percentage of cycle time? Might be more reasonable to encourage user to set?
        }
        if (!config.containsKey(ConsumerConfig.FETCH_MIN_BYTES_CONFIG)) {
            // Potential to configure for higher throughput; still adheres to max wait ms
        }
        if (!config.containsKey(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG)) {
            // The default of 1MiB seems too low
            // Overall limit still adheres to FETCH_MAX_BYTES_CONFIG (default 50MiB)
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
}
