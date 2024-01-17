/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.configuration.Configuration;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.deephaven.kafka.KafkaTools.KAFKA_PARTITION_COLUMN_NAME_DEFAULT;
import static io.deephaven.kafka.KafkaTools.KAFKA_PARTITION_COLUMN_NAME_PROPERTY;
import static io.deephaven.kafka.KafkaTools.KEY_BYTES_COLUMN_NAME_PROPERTY;
import static io.deephaven.kafka.KafkaTools.OFFSET_COLUMN_NAME_DEFAULT;
import static io.deephaven.kafka.KafkaTools.OFFSET_COLUMN_NAME_PROPERTY;
import static io.deephaven.kafka.KafkaTools.TIMESTAMP_COLUMN_NAME_DEFAULT;
import static io.deephaven.kafka.KafkaTools.TIMESTAMP_COLUMN_NAME_PROPERTY;
import static io.deephaven.kafka.KafkaTools.VALUE_BYTES_COLUMN_NAME_PROPERTY;

/**
 * Provides options for processing the common fields of {@link ConsumerRecord}.
 */
@Immutable
@BuildableStyle
public abstract class ConsumerRecordOptions {

    public static Builder builder() {
        return ImmutableConsumerRecordOptions.builder();
    }


    /**
     * The empty record options. Equivalent to {@code builder().build()}.
     *
     * @return the empty record options
     */
    public static ConsumerRecordOptions empty() {
        return builder().build();
    }

    /**
     * The latest record options. Currently, equivalent to {@link #v2()}; this may change from release to release.
     *
     * @return the latest record options
     */
    public static ConsumerRecordOptions latest() {
        return v2();
    }

    /**
     * Attempts to mimic the options for v1...
     * 
     * @return
     */
    public static ConsumerRecordOptions v1() {
        final Configuration config = Configuration.getInstance();
        final Builder builder = builder()
                .putFields(Field.PARTITION, config.getStringWithDefault(KAFKA_PARTITION_COLUMN_NAME_PROPERTY,
                        KAFKA_PARTITION_COLUMN_NAME_DEFAULT))
                .putFields(Field.OFFSET,
                        config.getStringWithDefault(OFFSET_COLUMN_NAME_PROPERTY, OFFSET_COLUMN_NAME_DEFAULT))
                .putFields(Field.TIMESTAMP,
                        config.getStringWithDefault(TIMESTAMP_COLUMN_NAME_PROPERTY, TIMESTAMP_COLUMN_NAME_DEFAULT));
        if (config.hasProperty(KEY_BYTES_COLUMN_NAME_PROPERTY)) {
            builder.putFields(Field.SERIALIZED_KEY_SIZE, config.getProperty(KEY_BYTES_COLUMN_NAME_PROPERTY));
        }
        if (config.hasProperty(VALUE_BYTES_COLUMN_NAME_PROPERTY)) {
            builder.putFields(Field.SERIALIZED_VALUE_SIZE, config.getProperty(VALUE_BYTES_COLUMN_NAME_PROPERTY));
        }
        return builder.build();
    }

    /**
     * Equivalent to
     *
     * <pre>
     * builder()
     *         .putFields(Field.TOPIC, "Topic")
     *         .putFields(Field.PARTITION, "Partition")
     *         .putFields(Field.OFFSET, "Offset")
     *         .putFields(Field.LEADER_EPOCH, "LeaderEpoch")
     *         .putFields(Field.TIMESTAMP_TYPE, "TimestampType")
     *         .putFields(Field.TIMESTAMP, "Timestamp")
     *         .putFields(Field.SERIALIZED_KEY_SIZE, "SerializedKeySize")
     *         .putFields(Field.SERIALIZED_VALUE_SIZE, "SerializedValueSize")
     *         .build()
     * </pre>
     *
     * @return the v2 options
     */
    public static ConsumerRecordOptions v2() {
        return builder()
                .putFields(Field.TOPIC, "Topic")
                .putFields(Field.PARTITION, "Partition")
                .putFields(Field.OFFSET, "Offset")
                .putFields(Field.LEADER_EPOCH, "LeaderEpoch")
                .putFields(Field.TIMESTAMP_TYPE, "TimestampType")
                .putFields(Field.TIMESTAMP, "Timestamp")
                .putFields(Field.SERIALIZED_KEY_SIZE, "SerializedKeySize")
                .putFields(Field.SERIALIZED_VALUE_SIZE, "SerializedValueSize")
                .build();
    }

    /**
     * A map from {@link Field} to column name.
     *
     * @return the fields
     */
    public abstract Map<Field, String> fields();

//    /**
//     * Equivalent to {@code fields().values()}.
//     *
//     * @return the column names
//     */
//    public final Collection<String> columnNames() {
//        return fields().values();
//    }

    public interface Builder {

        Builder putFields(Field key, String value);

        Builder putFields(Map.Entry<Field, ? extends String> entry);

        Builder putAllFields(Map<Field, ? extends String> entries);

        ConsumerRecordOptions build();
    }

    public enum Field {
        /**
         * The topic.
         *
         * @see ConsumerRecord#topic()
         */
        TOPIC(Type.stringType()),

        /**
         * The partition.
         *
         * @see ConsumerRecord#partition()
         */
        PARTITION(Type.intType()),

        /**
         * The offset.
         *
         * @see ConsumerRecord#offset()
         */
        OFFSET(Type.longType()),

        /**
         * The leader epoch.
         *
         * @see ConsumerRecordFunctions#leaderEpoch(ConsumerRecord)
         */
        LEADER_EPOCH(Type.intType()),

        /**
         * The timestamp type.
         *
         * @see ConsumerRecord#timestampType()
         */
        TIMESTAMP_TYPE(Type.ofCustom(TimestampType.class)),

        /**
         * The timestamp.
         *
         * @see ConsumerRecordFunctions#timestampEpochNanos(ConsumerRecord)
         */
        TIMESTAMP(Type.instantType()),

        /**
         * The serialized key size.
         *
         * @see ConsumerRecordFunctions#serializedKeySize(ConsumerRecord)
         */
        SERIALIZED_KEY_SIZE(Type.intType()),

        /**
         * The serialized value size.
         *
         * @see ConsumerRecordFunctions#serializedValueSize(ConsumerRecord)
         */
        SERIALIZED_VALUE_SIZE(Type.intType()),
        ;

        private final Type<?> type;

        Field(Type<?> type) {
            this.type = Objects.requireNonNull(type);
        }

        Type<?> type() {
            return type;
        }
    }

    @Derived
    List<Type<?>> outputTypes() {
        return fields().keySet().stream().map(Field::type).collect(Collectors.toList());
    }

    final <K, V> NamedObjectProcessor<ConsumerRecord<K, V>> namedObjectProcessor() {
        return NamedObjectProcessor.of(processor(), columnNames());
    }


    final <K, V> ObjectProcessor<ConsumerRecord<K, V>> processor() {
        return fields().isEmpty() ? ObjectProcessor.empty() : new ConsumerRecordOptionsProcessor<>();
    }

    final Collection<String> columnNames() {
        return fields().values();
    }

    private boolean has(Field field) {
        return fields().containsKey(field);
    }

    private class ConsumerRecordOptionsProcessor<K, V> implements ObjectProcessor<ConsumerRecord<K, V>> {
        @Override
        public List<Type<?>> outputTypes() {
            return ConsumerRecordOptions.this.outputTypes();
        }

        @Override
        public void processAll(ObjectChunk<? extends ConsumerRecord<K, V>, ?> in, List<WritableChunk<?>> out) {
            int ix = 0;
            final WritableObjectChunk<String, ?> topics = has(Field.TOPIC)
                    ? out.get(ix++).asWritableObjectChunk()
                    : null;
            final WritableIntChunk<?> partitions = has(Field.PARTITION)
                    ? out.get(ix++).asWritableIntChunk()
                    : null;
            final WritableLongChunk<?> offsets = has(Field.OFFSET)
                    ? out.get(ix++).asWritableLongChunk()
                    : null;
            final WritableIntChunk<?> leaderEpochs = has(Field.LEADER_EPOCH)
                    ? out.get(ix++).asWritableIntChunk()
                    : null;
            final WritableObjectChunk<TimestampType, ?> timestampTypes = has(Field.TIMESTAMP_TYPE)
                    ? out.get(ix++).asWritableObjectChunk()
                    : null;
            final WritableLongChunk<?> timestamps = has(Field.TIMESTAMP)
                    ? out.get(ix++).asWritableLongChunk()
                    : null;
            final WritableIntChunk<?> serializedKeySize = has(Field.SERIALIZED_KEY_SIZE)
                    ? out.get(ix++).asWritableIntChunk()
                    : null;
            final WritableIntChunk<?> serializedValueSize = has(Field.SERIALIZED_VALUE_SIZE)
                    ? out.get(ix++).asWritableIntChunk()
                    : null;
            for (int i = 0; i < in.size(); ++i) {
                final ConsumerRecord<?, ?> record = in.get(i);
                if (topics != null) {
                    topics.add(record.topic());
                }
                if (partitions != null) {
                    partitions.add(record.partition());
                }
                if (offsets != null) {
                    offsets.add(record.offset());
                }
                if (leaderEpochs != null) {
                    leaderEpochs.add(ConsumerRecordFunctions.leaderEpoch(record));
                }
                if (timestampTypes != null) {
                    timestampTypes.add(record.timestampType());
                }
                if (timestamps != null) {
                    timestamps.add(ConsumerRecordFunctions.timestampEpochNanos(record));
                }
                if (serializedKeySize != null) {
                    serializedKeySize.add(ConsumerRecordFunctions.serializedKeySize(record));
                }
                if (serializedValueSize != null) {
                    serializedValueSize.add(ConsumerRecordFunctions.serializedValueSize(record));
                }
            }
        }
    }
}
