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
import io.deephaven.kafka.v2.ConsumerRecordOptions.Field;
import io.deephaven.kafka.v2.PublishersOptions.Partitioning;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.util.SafeCloseable;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
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

    // todo: should this be a list?
    /**
     * The offsets.
     *
     * @return the offsets
     */
    public abstract Offsets offsets();

    /**
     * The {@link ConsumerRecord} filter. The filtering happens before processing. By default, is equivalent to
     * {@code record -> true}, which will include all records.
     *
     * @return the record filter
     */
    @Default
    public Predicate<ConsumerRecord<K, V>> filter() {
        return predicateTrue();
    }

    /**
     * The basic {@link ConsumerRecord} options for a {@link ConsumerRecord} processor. By default, is equivalent to
     * {@link ConsumerRecordOptions#latest()}. Callers wishing to ensure compatibility across releases are encourage to
     * set this to a specifically versioned {@link ConsumerRecordOptions}.
     *
     * @return the record options
     */
    @Default
    public ConsumerRecordOptions recordOptions() {
        return ConsumerRecordOptions.latest();
    }

    @Default
    public boolean useOpinionatedRecordOptions() {
        return true;
    }

    /**
     * The {@link ConsumerRecord} processor. This is for advanced use cases where the caller wants additional fields for
     * the high-level {@link ConsumerRecord} object (for example, "how many headers are there?" or "what is the value of
     * header X"), or the caller wants to directly parse the key / value without the layer of mapping that
     * {@link #keyProcessor()} / {@link #valueProcessor()} uses.
     *
     * @return the record processor
     */
    public abstract Optional<NamedObjectProcessor<ConsumerRecord<K, V>>> recordProcessor();

    /**
     * When present, the key processor is adapted into a {@link ConsumerRecord} processor via
     * {@link Processors#key(ObjectProcessor)}.
     *
     * @return the key processor
     */
    public abstract Optional<NamedObjectProcessor<K>> keyProcessor();

    /**
     * When present, the value processor is adapted into a {@link ConsumerRecord} processor via
     * {@link Processors#value(ObjectProcessor)}.
     *
     * @return the value processor
     */
    public abstract Optional<NamedObjectProcessor<V>> valueProcessor();

    /**
     * The table type for ...
     *
     * @return the table type
     */
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

        Builder<K, V> useOpinionatedRecordOptions(boolean useOpinionatedRecordOptions);

        default Builder<K, V> recordProcessor(ObjectProcessor<ConsumerRecord<K, V>> processor) {
            final int size = processor.size();
            if (size == 1) {
                return recordProcessor(NamedObjectProcessor.of(processor, "Record"));
            }
            final List<String> names = new ArrayList<>(size);
            for (int i = 0; i < size; ++i) {
                names.add("Record_" + i);
            }
            return recordProcessor(NamedObjectProcessor.of(processor, names));
        }

        Builder<K, V> recordProcessor(NamedObjectProcessor<ConsumerRecord<K, V>> processor);

        default Builder<K, V> keyProcessor(ObjectProcessor<K> processor) {
            final int size = processor.size();
            if (size == 1) {
                return keyProcessor(NamedObjectProcessor.of(processor, "Key"));
            }
            final List<String> names = new ArrayList<>(size);
            for (int i = 0; i < size; ++i) {
                names.add("Key_" + i);
            }
            return keyProcessor(NamedObjectProcessor.of(processor, names));
        }

        Builder<K, V> keyProcessor(NamedObjectProcessor<K> processor);

        default Builder<K, V> valueProcessor(ObjectProcessor<V> processor) {
            final int size = processor.size();
            if (size == 1) {
                return valueProcessor(NamedObjectProcessor.of(processor, "Value"));
            }
            final List<String> names = new ArrayList<>(size);
            for (int i = 0; i < size; ++i) {
                names.add("Value_" + i);
            }
            return valueProcessor(NamedObjectProcessor.of(processor, names));
        }

        Builder<K, V> valueProcessor(NamedObjectProcessor<V> processor);

        Builder<K, V> tableType(TableType tableType);

        Builder<K, V> putExtraAttributes(String key, Object value);

        Builder<K, V> putExtraAttributes(Map.Entry<String, ?> entry);

        Builder<K, V> putAllExtraAttributes(Map<String, ?> entries);

        Builder<K, V> updateSourceRegistrar(UpdateSourceRegistrar updateSourceRegistrar);

        Builder<K, V> chunkSize(int chunkSize);

        Builder<K, V> receiveTimestamp(String receiveTimestamp);

        TableOptions<K, V> build();
    }

    @Derived
    ConsumerRecordOptions recordOptionsToUse() {
        // This method should be the only (internal) caller of recordOptions()
        if (!useOpinionatedRecordOptions()) {
            return recordOptions();
        }
        final ConsumerRecordOptions.Builder builder = ConsumerRecordOptions.builder();
        final Map<Field, String> fields = recordOptions().fields();
        if (!fields.containsKey(Field.TOPIC) && ((OffsetsBase) offsets()).topics().distinct().count() > 1) {
            // todo: may not want / be relevant in partitioned case?
            builder.addField(Field.TOPIC);
        }
        if (!fields.containsKey(Field.SERIALIZED_KEY_SIZE) && keyProcessor().isPresent()) {
            builder.addField(Field.SERIALIZED_KEY_SIZE);
        }
        if (!fields.containsKey(Field.SERIALIZED_VALUE_SIZE) && valueProcessor().isPresent()) {
            builder.addField(Field.SERIALIZED_VALUE_SIZE);
        }
        return builder.putAllFields(recordOptions().fields()).build();
    }

    @Check
    final void checkChunkSize() {
        if (chunkSize() < 1) {
            throw new IllegalArgumentException("chunkSize must be positive");
        }
    }

    final Table table() {
        return Publishers.applyAndStart(publishersOptions(Partitioning.single()), this::singleTable);
    }

    final PartitionedTable partitionedTable() {
        return Publishers.applyAndStart(publishersOptions(Partitioning.perTopicPartition()), this::partitionedTable);
    }

    final TableDefinition tableDefinition() {
        final List<NamedObjectProcessor<?>> namedProcessors = namedProcessors().collect(Collectors.toList());
        final List<String> columnNames = Stream.concat(
                Stream.ofNullable(receiveTimestamp()),
                namedProcessors.stream().map(NamedObjectProcessor::columnNames).flatMap(Collection::stream))
                .collect(Collectors.toList());
        final List<Type<?>> outputTypes = Stream.concat(
                receiveTimestamp() == null ? Stream.empty() : Stream.of(Type.instantType()),
                namedProcessors.stream()
                        .map(NamedObjectProcessor::processor)
                        .map(ObjectProcessor::outputTypes)
                        .flatMap(Collection::stream))
                .collect(Collectors.toList());
        return TableDefinition.from(columnNames, outputTypes);
    }

    private Stream<NamedObjectProcessor<?>> namedProcessors() {
        return Stream.concat(
                Stream.of(recordOptionsToUse().namedProcessor()),
                Stream.of(
                        recordProcessor(),
                        keyProcessor(),
                        valueProcessor())
                        .flatMap(Optional::stream));
    }

    private Stream<ObjectProcessor<ConsumerRecord<K, V>>> objectProcessors() {
        return Stream.concat(
                Stream.of(recordOptionsToUse().processor()),
                Stream.of(
                        recordProcessor().map(NamedObjectProcessor::processor),
                        mappedKeyProcessor(),
                        mappedValueProcessor())
                        .flatMap(Optional::stream));
    }

    private Optional<ObjectProcessor<ConsumerRecord<K, V>>> mappedKeyProcessor() {
        return keyProcessor().map(NamedObjectProcessor::processor).map(Processors::key);
    }

    private Optional<ObjectProcessor<ConsumerRecord<K, V>>> mappedValueProcessor() {
        return valueProcessor().map(NamedObjectProcessor::processor).map(Processors::value);
    }

    private ObjectProcessor<ConsumerRecord<K, V>> consumerRecordObjectProcessor() {
        return ObjectProcessor.combined(objectProcessors().collect(Collectors.toList()));
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


    private Table singleTable(Collection<? extends Publisher> publishers) {
        return toTableType(streamConsumer(single(publishers)).table());
    }

    private StreamToBlinkTableAdapter streamConsumer(Publisher publisher) {
        if (publisher.topicPartitions().isEmpty()) {
            throw new IllegalArgumentException();
        }
        final Map<String, Object> extraAttributes;
        if (publisher.topicPartitions().size() == 1
                && recordOptionsToUse().fields().containsKey(Field.OFFSET)
                && !extraAttributes().containsKey(Table.SORTED_COLUMNS_ATTRIBUTE)) {
            final String value = SortedColumnsAttribute
                    .setOrderForColumn((String) null, recordOptionsToUse().fields().get(Field.OFFSET),
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
                    // Even though rawTable won't change, we can't set refreshing as false
                    setFlat();
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
                .clientOptions(clientOptionsToUse())
                .partitioning(partitioning)
                .offsets(offsets())
                .filter(filter())
                .processor(consumerRecordObjectProcessor())
                .chunkSize(chunkSize())
                .receiveTimestamp(receiveTimestamp() != null)
                .build();
    }

    // Note: not Derived like recordOptionsToUse; if we end up needing to call clientOptionsToUse more than once, may
    // make sense to make Derived.
    private ClientOptions<K, V> clientOptionsToUse() {
        // This method should be the only (internal) caller of clientOptions()
        if (!useOpinionatedClientOptions()) {
            return clientOptions();
        }
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
