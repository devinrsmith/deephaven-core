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
import org.immutables.value.Value.Auxiliary;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Lazy;

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

/**
 * The options for constructing a {@link Table} or {@link PartitionedTable} from a Kafka stream source.
 *
 * @param <K> the Kafka stream key type
 * @param <V> the Kafka stream value type
 * @see Tablez#of(TableOptions)
 * @see Tablez#ofPartitioned(TableOptions)
 */
@Immutable
@BuildableStyle
public abstract class TableOptions<K, V> {
    public static <K, V> Builder<K, V> builder() {
        return ImmutableTableOptions.builder();
    }

    /**
     * The name. Defaults to a random string.
     *
     * @return the name
     */
    @Default
    public String name() {
        return randomId();
    }

    /**
     * The Kafka client options.
     *
     * @return the Kafka client options
     */
    public abstract ClientOptions<K, V> clientOptions();

    /**
     * Opinionated options on top of {@link #clientOptions()}.
     */
    public interface OpinionatedClientOptions {

        /**
         * Do not use opinionated client options.
         *
         * @return no opinionated client options
         */
        static OpinionatedClientOptions none() {
            return ClientOpts.NONE;
        }

        /**
         * <ul>
         * <li>If unset, sets {@link ConsumerConfig#CLIENT_ID_CONFIG} to a random string (the same random string as
         * {@link #name()} if it hasn't been explicitly set).</li>
         * <li>If unset, sets {@link ConsumerConfig#DEFAULT_API_TIMEOUT_MS_CONFIG} to 5 seconds.</li>
         * <li>If unset, sets {@link ConsumerConfig#MAX_POLL_RECORDS_CONFIG} to {@link #chunkSize()}.</li>
         * <li>If unset, sets {@link ConsumerConfig#MAX_PARTITION_FETCH_BYTES_CONFIG} to 16 MiB.</li>
         * <li>If unset, sets {@link ClientOptions#keyDeserializer()} to {@link ByteArrayDeserializer}
         * ({@link ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG} must also be unset).</li>
         * <li>If unset, sets {@link ClientOptions#valueDeserializer()} to {@link ByteArrayDeserializer}
         * ({@link ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG} must also be unset).</li>
         * </ul>
         *
         * @return opinionated client options v1
         */
        static OpinionatedClientOptions v1() {
            return ClientOpts.V1;
        }

        /**
         * The latest opinionated client options. Currently, equivalent to {@link #v1()}.
         *
         * @return the latest opinionated client options
         */
        static OpinionatedClientOptions latest() {
            return v1();
        }
    }

    /**
     * The logic to use for opinionated client configuration. By default, is equivalent to
     * {@link OpinionatedClientOptions#latest()}. Callers wishing to do their own optimizations are encouraged to set
     * this to {@link OpinionatedClientOptions#none()} and finely configure {@link #clientOptions()}.
     *
     * @return the opinionated client configuration logic
     */
    @Default
    public OpinionatedClientOptions opinionatedClientOptions() {
        return OpinionatedClientOptions.latest();
    }

    // todo: should this be a list?
    /**
     * The offsets.
     *
     * @return the offsets
     */
    public abstract List<Offsets> offsets();

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
     * The basic {@link ConsumerRecord} options for a {@link ConsumerRecord} processor. By default, is
     * {@link ConsumerRecordOptions#latest()}. Callers wishing to ensure compatibility across releases are encourage to
     * set this to a specifically versioned {@link ConsumerRecordOptions}.
     *
     * @return the record options
     */
    @Default
    public ConsumerRecordOptions recordOptions() {
        return ConsumerRecordOptions.latest();
    }

    /**
     * Opinionated options on top of {@link #recordOptions()}.
     */
    public interface OpinionatedRecordOptions {

        /**
         * Do not use opinionated record options.
         *
         * @return no opinionated record options
         */
        static OpinionatedRecordOptions none() {
            return RecordOpts.NONE;
        }

        /**
         *
         * <ul>
         * <li>If {@link Field#TOPIC} is unset and {@link #offsets()} contains 2 or more topics, will add in
         * {@link Field#TOPIC}</li>
         * <li>If {@link Field#KEY_SIZE} is unset and {@link #keyProcessor()} is present, will add in
         * {@link Field#KEY_SIZE}</li>
         * <li>If {@link Field#VALUE_SIZE} is unset and {@link #valueProcessor()} is present, will add in
         * {@link Field#KEY_SIZE}</li>
         * </ul>
         *
         * @return the opinionated record options v1
         */
        static OpinionatedRecordOptions v1() {
            return RecordOpts.V1;
        }

        /**
         * The latest opinionated client options. Currently, equivalent to {@link #v1()}.
         *
         * @return the latest opinionated record options
         */
        static OpinionatedRecordOptions latest() {
            return v1();
        }
    }

    /**
     * The logic to use for opinionated client configuration. By default, is equivalent to
     * {@link OpinionatedRecordOptions#latest()}. Callers wishing to do their own configuration are encouraged to set
     * this to {@link OpinionatedRecordOptions#none()} and finely configure {@link #recordOptions()} or
     * {@link #recordProcessor()}.
     *
     * @return the opinionated record configuration logic
     */
    @Default
    public OpinionatedRecordOptions opinionatedRecordOptions() {
        return OpinionatedRecordOptions.latest();
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
    public abstract Optional<NamedObjectProcessor<? super K>> keyProcessor();

    /**
     * When present, the value processor is adapted into a {@link ConsumerRecord} processor via
     * {@link Processors#value(ObjectProcessor)}.
     *
     * @return the value processor
     */
    public abstract Optional<NamedObjectProcessor<? super V>> valueProcessor();

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

        Builder<K, V> opinionatedClientOptions(OpinionatedClientOptions opinionatedClientOptions);

        Builder<K, V> addOffsets(Offsets element);

        Builder<K, V> addOffsets(Offsets... elements);

        Builder<K, V> addAllOffsets(Iterable<? extends Offsets> elements);

        Builder<K, V> filter(Predicate<ConsumerRecord<K, V>> filter);

        Builder<K, V> recordOptions(ConsumerRecordOptions recordOptions);

        Builder<K, V> opinionatedRecordOptions(OpinionatedRecordOptions opinionatedRecordOptions);

        default Builder<K, V> recordProcessor(ObjectProcessor<ConsumerRecord<K, V>> processor) {
            return recordProcessor(NamedObjectProcessor.prefix(processor, "Record"));
        }

        Builder<K, V> recordProcessor(NamedObjectProcessor<ConsumerRecord<K, V>> processor);

        default Builder<K, V> keyProcessor(ObjectProcessor<? super K> processor) {
            return keyProcessor(NamedObjectProcessor.prefix(processor, "Key"));
        }

        Builder<K, V> keyProcessor(NamedObjectProcessor<? super K> processor);

        default Builder<K, V> valueProcessor(ObjectProcessor<? super V> processor) {
            return valueProcessor(NamedObjectProcessor.prefix(processor, "Value"));
        }

        Builder<K, V> valueProcessor(NamedObjectProcessor<? super V> processor);

        Builder<K, V> tableType(TableType tableType);

        Builder<K, V> putExtraAttributes(String key, Object value);

        Builder<K, V> putExtraAttributes(Map.Entry<String, ?> entry);

        Builder<K, V> putAllExtraAttributes(Map<String, ?> entries);

        Builder<K, V> updateSourceRegistrar(UpdateSourceRegistrar updateSourceRegistrar);

        Builder<K, V> chunkSize(int chunkSize);

        Builder<K, V> receiveTimestamp(String receiveTimestamp);

        TableOptions<K, V> build();
    }

    @Lazy
    String randomId() {
        // Internal use only - default for name() and client.id.
        return UUID.randomUUID().toString();
    }

    @Derived
    @Auxiliary
    ConsumerRecordOptions recordOptionsToUse() {
        switch ((RecordOpts) opinionatedRecordOptions()) {
            case NONE:
                return recordOptions();
            case V1:
                return recordOptionsV1();
        }
        throw new IllegalStateException();
    }

    // Note: not Derived like recordOptionsToUse; if we end up needing to call clientOptionsToUse more than once, may
    // make sense to make Derived.
    private ClientOptions<K, V> clientOptionsToUse() {
        switch (((ClientOpts) opinionatedClientOptions())) {
            case NONE:
                return clientOptions();
            case V1:
                return clientOptionsV1();
        }
        throw new IllegalStateException();
    }

    @Check
    final void checkChunkSize() {
        if (chunkSize() < 1) {
            throw new IllegalArgumentException("chunkSize must be positive");
        }
    }

    @Check
    final void checkOpinionatedClientOptions() {
        if (!(opinionatedClientOptions() instanceof ClientOpts)) {
            throw new IllegalArgumentException("Unsupported");
        }
    }

    @Check
    final void checkOpinionatedRecordOptions() {
        if (!(opinionatedRecordOptions() instanceof RecordOpts)) {
            throw new IllegalArgumentException("Unsupported");
        }
    }

    @Check
    final void checkOffsets() {
        if (offsets().isEmpty()) {
            throw new IllegalArgumentException("Offsets is empty");
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
        // noinspection resource
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

    private static final ColumnDefinition<String> TOPIC_COLUMN = ColumnDefinition.ofString("Topic").withPartitioning();
    private static final ColumnDefinition<Integer> PARTITION_COLUMN =
            ColumnDefinition.ofInt("Partition").withPartitioning();
    private static final ColumnDefinition<Table> TABLE_COLUMN = ColumnDefinition.fromGenericType("Table", Table.class);
    private static final TableDefinition TOPIC_PARTITION_COLUMN_TABLEDEF =
            TableDefinition.of(TOPIC_COLUMN, PARTITION_COLUMN, TABLE_COLUMN);

    private PartitionedTable partitionedTable(Collection<? extends Publisher> publishers) {
        final List<Publisher> sorted = new ArrayList<>(publishers);
        sorted.sort(Comparator.comparing(TableOptions::singleTopicPartition, ClientHelper.TOPIC_PARTITION_COMPARATOR));
        final int numPartitions = sorted.size();
        final String[] topics = new String[numPartitions];
        final int[] partitions = new int[numPartitions];
        final Table[] constituents = new Table[numPartitions];
        for (int i = 0; i < sorted.size(); i++) {
            final TopicPartition topicPartition = singleTopicPartition(sorted.get(i));
            topics[i] = topicPartition.topic();
            partitions[i] = topicPartition.partition();
            // noinspection resource
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
                .offsets(Offsets.of(offsets()))
                .filter(filter())
                .processor(consumerRecordObjectProcessor())
                .chunkSize(chunkSize())
                .receiveTimestamp(receiveTimestamp() != null)
                .build();
    }

    private ClientOptions<K, V> clientOptionsV1() {
        final HashMap<String, String> config = new HashMap<>(clientOptions().config());
        final ClientOptions.Builder<K, V> opinionated = ClientOptions.builder();
        if (!config.containsKey(ConsumerConfig.CLIENT_ID_CONFIG)) {
            config.put(ConsumerConfig.CLIENT_ID_CONFIG, randomId());
        }
        if (!config.containsKey(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)) {
            // This is local only option, doesn't affect server.
            // Default is 500
            // This affects the maximum number of records that io.deephaven.kafka.v2.KafkaPublisherDriver.runOnce will
            // receive at once. There's a small tradeoff here; allowing enough for runOnce to do a flush, but also small
            // enough to minimize potential sync wait for StreamPublisher#flush calls (cycle).
            // Maybe this should be changed depending on the partitioning strategy?
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

    private ConsumerRecordOptions recordOptionsV1() {
        final Map<Field, String> fields = recordOptions().fields();
        final ConsumerRecordOptions.Builder builder = ConsumerRecordOptions.builder();
        if (!fields.containsKey(Field.TOPIC) && ((OffsetsBase) offsets()).topics().distinct().count() > 1) {
            // todo: may not want / be relevant in partitioned case?
            builder.addField(Field.TOPIC);
        }
        if (!fields.containsKey(Field.KEY_SIZE) && keyProcessor().isPresent()) {
            builder.addField(Field.KEY_SIZE);
        }
        if (!fields.containsKey(Field.VALUE_SIZE) && valueProcessor().isPresent()) {
            builder.addField(Field.VALUE_SIZE);
        }
        return builder.putAllFields(fields).build();
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

    private enum ClientOpts implements OpinionatedClientOptions {
        NONE, V1,
    }

    private enum RecordOpts implements OpinionatedRecordOptions {
        NONE, V1,
    }
}
