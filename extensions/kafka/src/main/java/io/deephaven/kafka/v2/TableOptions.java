/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.BlinkTableTools;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.SortedColumnsAttribute;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.partitioned.PartitionedTableImpl;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.ring.RingTableTools;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.kafka.KafkaTools.ConsumerLoopCallback;
import io.deephaven.kafka.KafkaTools.TableType;
import io.deephaven.kafka.KafkaTools.TableType.Append;
import io.deephaven.kafka.KafkaTools.TableType.Blink;
import io.deephaven.kafka.KafkaTools.TableType.Ring;
import io.deephaven.kafka.v2.PublishersOptions.Partitioning;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.thread.NamingThreadFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class TableOptions<K, V> {
    public static <K, V> Builder<K, V> builder() {
        return ImmutableTableOptions.builder();
    }

    // table output?
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

    /**
     * The consumer record options for for a {@link ConsumerRecord} processor.
     *
     * @return the record options
     */
    @Default
    public ConsumerRecordOptions recordOptions() {
        return ConsumerRecordOptions.of();
    }

    /**
     *
     * @return
     */
    public abstract Optional<ObjectProcessor<ConsumerRecord<K, V>>> recordProcessor();

    /**
     * When present, the key processor is adapted into a {@link ConsumerRecord} processor via {@link Processors#key(ObjectProcessor)}.
     *
     * @return
     */
    public abstract Optional<ObjectProcessor<K>> keyProcessor();

    /**
     * When present, the value processor is adapted into a {@link ConsumerRecord} processor via {@link Processors#value(ObjectProcessor)}.
     *
     * @return
     */
    public abstract Optional<ObjectProcessor<V>> valueProcessor();

    public abstract List<String> columnNames();

    @Default
    public TableType tableType() {
        return TableType.blink();
    }

    /**
     * The extra attributes to set on the underlying blink table.
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

    // todo: name?
    @Default
    @Nullable
    public String receiveTimestamp() {
        return "ReceiveTimestamp";
    }

    public abstract Optional<ConsumerLoopCallback> callback();

    public interface Builder<K, V> {
        Builder<K, V> name(String name);

        Builder<K, V> clientOptions(ClientOptions<K, V> clientOptions);

        Builder<K, V> useOpinionatedClientOptions(boolean useOpinionatedClientOptions);

        Builder<K, V> offsets(Offsets offsets);

        Builder<K, V> filter(Predicate<ConsumerRecord<K, V>> filter);

        Builder<K, V> recordOptions(ConsumerRecordOptions recordOptions);

        Builder<K, V> recordProcessor(ObjectProcessor<ConsumerRecord<K, V>> processor);

        Builder<K, V> keyProcessor(ObjectProcessor<K> processor);

        Builder<K, V> valueProcessor(ObjectProcessor<V> processor);

        Builder<K, V> addColumnNames(String element);

        Builder<K, V> addColumnNames(String... elements);

        Builder<K, V> addAllColumnNames(Iterable<String> elements);

        Builder<K, V> tableType(TableType tableType);

        Builder<K, V> putExtraAttributes(String key, Object value);

        Builder<K, V> putExtraAttributes(Map.Entry<String, ?> entry);

        Builder<K, V> putAllExtraAttributes(Map<String, ?> entries);

        Builder<K, V> updateSourceRegistrar(UpdateSourceRegistrar updateSourceRegistrar);

        Builder<K, V> chunkSize(int chunkSize);

        Builder<K, V> receiveTimestamp(String receiveTimestamp);

        TableOptions<K, V> build();
    }

    // @Check
    // final void checkSize() {
    // if (recordProcessor().size() != columnNames().size()) {
    // throw new IllegalArgumentException();
    // }
    // }

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
        return toTableType(streamConsumer().table());
    }

    final PartitionedTable partitionedTable() {
        // todo: wrap
        return Publishers.applyAndStart(publishersOptions(Partitioning.perTopicPartition()), this::partitionedTable);
    }

    final TableDefinition tableDefinition() {

        final List<String> extraNames = receiveTimestamp() == null
                ? Collections.emptyList()
                : Collections.singletonList(receiveTimestamp());

        final List<String> columnNames = Stream.of(
                extraNames.stream(),
                recordOptions().columnNames(),
                columnNames().stream())
                .flatMap(Function.identity())
                .collect(Collectors.toList());

        final List<Type<?>> extraTypes = receiveTimestamp() == null
                ? Collections.emptyList()
                : Collections.singletonList(Type.instantType());

        final List<Type<?>> outputTypes = Stream.of(
                extraTypes,
                recordOptions().processor().outputTypes(),
                recordProcessor().map(ObjectProcessor::outputTypes).orElseGet(Collections::emptyList),
                keyProcessor().map(ObjectProcessor::outputTypes).orElseGet(Collections::emptyList),
                valueProcessor().map(ObjectProcessor::outputTypes).orElseGet(Collections::emptyList))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        return TableDefinition.from(columnNames, outputTypes);
    }

    private ObjectProcessor<ConsumerRecord<K, V>> fullProcessor() {
        final ObjectProcessor<ConsumerRecord<K, V>> basics = recordOptions().processor();
        final ObjectProcessor<ConsumerRecord<K, V>> specifics = recordProcessor().orElseGet(ObjectProcessor::empty);
        final ObjectProcessor<ConsumerRecord<K, V>> key =
                Processors.key(keyProcessor().orElseGet(ObjectProcessor::empty));
        final ObjectProcessor<ConsumerRecord<K, V>> value =
                Processors.value(valueProcessor().orElseGet(ObjectProcessor::empty));
        return ObjectProcessor.combined(List.of(basics, specifics, key, value));
    }

    private Table toTableType(Table blinkTable) {
        return tableType().walk(new ToTableTypeVisitor(blinkTable));
    }

    private static class ToTableTypeVisitor implements TableType.Visitor<Table> {

        private final Table blinkTable;

        public ToTableTypeVisitor(Table blinkTable) {
            this.blinkTable = Objects.requireNonNull(blinkTable);
        }

        @Override
        public Table visit(Blink blink) {
            return blinkTable;
        }

        @Override
        public Table visit(Append append) {
            return BlinkTableTools.blinkToAppendOnly(blinkTable);
        }

        @Override
        public Table visit(Ring ring) {
            return RingTableTools.of(blinkTable, ring.capacity());
        }
    }

    private StreamToBlinkTableAdapter streamConsumer() {
        return Publishers.applyAndStart(publishersOptions(Partitioning.single()), this::streamConsumer);
    }

    private StreamToBlinkTableAdapter streamConsumer(Collection<? extends Publisher> publishers) {
        return streamConsumer(single(publishers));
    }

    private StreamToBlinkTableAdapter streamConsumer(Publisher publisher) {
        if (publisher.topicPartitions().isEmpty()) {
            throw new IllegalArgumentException();
        }
        final Map<String, Object> extraAttributes;
        if (publisher.topicPartitions().size() == 1
                && recordOptions().offset() != null
                && !extraAttributes().containsKey(Table.SORTED_COLUMNS_ATTRIBUTE)) {
            final String value = SortedColumnsAttribute.setOrderForColumn((String) null, recordOptions().offset(),
                    SortingOrder.Ascending);
            final Map<String, Object> withSorted = new HashMap<>(extraAttributes());
            withSorted.put(Table.SORTED_COLUMNS_ATTRIBUTE, value);
            extraAttributes = withSorted;
        } else {
            extraAttributes = extraAttributes();
        }
        return new StreamToBlinkTableAdapter(
                tableDefinition(),
                publisher,
                updateSourceRegistrar(),
                name(),
                extraAttributes);
    }

    private static final Comparator<TopicPartition> TOPIC_PARTITION_COMPARATOR =
            Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition);

    private static final ColumnDefinition<String> TOPIC_COLUMN = ColumnDefinition.ofString("Topic").withPartitioning();
    private static final ColumnDefinition<Integer> PARTITION_COLUMN =
            ColumnDefinition.ofInt("Partition").withPartitioning();
    private static final ColumnDefinition<Table> TABLE_COLUMN = ColumnDefinition.fromGenericType("Table", Table.class);
    private static final TableDefinition TOPIC_PARTITION_COLUMN_TABLEDEF =
            TableDefinition.of(TOPIC_COLUMN, PARTITION_COLUMN, TABLE_COLUMN);

    private PartitionedTable partitionedTable(Collection<? extends Publisher> publishers) {
        final List<Publisher> sorted = new ArrayList<>(publishers);
        sorted.sort(Comparator.comparing(TableOptions::singleTopicPartition, TOPIC_PARTITION_COMPARATOR));
        final int numPartitions = sorted.size();
        final String[] topics = new String[numPartitions];
        final int[] partitions = new int[numPartitions];
        final Table[] constituents = new Table[numPartitions];
        for (int i = 0; i < sorted.size(); i++) {
            final TopicPartition topicPartition = singleTopicPartition(sorted.get(i));
            topics[i] = topicPartition.topic();
            partitions[i] = topicPartition.partition();
            // todo: should mark that offset is sorted, if it exists
            constituents[i] = toTableType(streamConsumer(sorted.get(i)).table());
        }
        // should we consider that Topic is "grouped"? NO
        // could add attributes to say that sorted on (topic, partition)
        // io.deephaven.engine.table.impl.SortedColumnsAttribute
        // might need to set it the constituent as well - set it on KafkaOffset

        // noinspection resource
        final TrackingWritableRowSet rowSet = RowSetFactory.flat(numPartitions).toTracking();
        final Map<String, ColumnSource<?>> sources = new LinkedHashMap<>(3) {
            {
                put(TOPIC_COLUMN.getName(), InMemoryColumnSource.getImmutableMemoryColumnSource(topics));
                put(PARTITION_COLUMN.getName(), InMemoryColumnSource.getImmutableMemoryColumnSource(partitions));
                put(TABLE_COLUMN.getName(), InMemoryColumnSource.getImmutableMemoryColumnSource(constituents));
            }
        };
        try (final SafeCloseable ignored = ExecutionContext.getContext()
                .withUpdateGraph(updateSourceRegistrar().getUpdateGraph(constituents))
                .open()) {
            final Table rawTable = new QueryTable(TOPIC_PARTITION_COLUMN_TABLEDEF, rowSet, sources) {
                {
                    // Can't set rawTable as refreshing == false;
                    // setRefreshing(false);
                    setFlat();
                    // todo: extra attributes?
                }
            };
            for (Table constituent : constituents) {
                rawTable.addParentReference(constituent);
            }
            return new PartitionedTableImpl(
                    rawTable,
                    List.of(TOPIC_COLUMN.getName(), PARTITION_COLUMN.getName()),
                    true,
                    TABLE_COLUMN.getName(),
                    tableDefinition(),
                    false,
                    false);
        }
    }

    private static TopicPartition singleTopicPartition(Publisher p) {
        return single(p.topicPartitions());
    }

    private PublishersOptions<K, V> publishersOptions(Partitioning partitioning) {
        return PublishersOptions.<K, V>builder()
                .clientOptions(useOpinionatedClientOptions() ? opinionatedClientOptions() : clientOptions())
                .partitioning(partitioning)
                .offsets(offsets())
                .filter(filter())
                .processor(fullProcessor())
                .chunkSize(chunkSize())
                .receiveTimestamp(receiveTimestamp() != null)
                .build();
    }

    private ClientOptions<K, V> opinionatedClientOptions() {
        final HashMap<String, String> config = new HashMap<>(clientOptions().config());
        final ClientOptions.Builder<K, V> opinionated = ClientOptions.builder();
        if (!config.containsKey(ConsumerConfig.CLIENT_ID_CONFIG)) {
            config.put(ConsumerConfig.CLIENT_ID_CONFIG, name());
        }
        if (!config.containsKey(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)) {
            // This is local only option, doesn't affect server.
            // Default is 500
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
        return new NamingThreadFactory(null, TableOptions.class, "KafkaPublisherDriver", true);
    }

    private enum PredicateTrue implements Predicate<Object> {
        PREDICATE_TRUE;

        @Override
        public boolean test(Object o) {
            return true;
        }
    }

    static <T> Predicate<T> predicateTrue() {
        // noinspection unchecked
        return (Predicate<T>) PredicateTrue.PREDICATE_TRUE;
    }

    private static <T> T single(Collection<T> collection) {
        final Iterator<T> it = collection.iterator();
        if (!it.hasNext()) {
            throw new IllegalStateException();
        }
        final T publisher = it.next();
        if (it.hasNext()) {
            throw new IllegalStateException();
        }
        return publisher;
    }
}
