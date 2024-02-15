/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.function.ToIntFunction;
import io.deephaven.function.ToLongFunction;
import io.deephaven.function.ToObjectFunction;
import io.deephaven.function.TypedFunction;
import io.deephaven.qst.type.Type;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@Immutable
@BuildableStyle
public abstract class KafkaRecordColumns {
    private static final ToObjectFunction<ConsumerRecord<?, ?>, String> TOPIC =
            ToObjectFunction.of(ConsumerRecord::topic, Type.stringType());
    private static final ToIntFunction<ConsumerRecord<?, ?>> PARTITION = ConsumerRecord::partition;
    private static final ToLongFunction<ConsumerRecord<?, ?>> OFFSET = ConsumerRecord::offset;
    private static final ToObjectFunction<ConsumerRecord<?, ?>, Instant> TIMESTAMP =
            ToObjectFunction.of(KafkaRecordColumns::timestamp, Type.instantType());
    private static final ToObjectFunction<ConsumerRecord<?, ?>, TimestampType> TIMESTAMP_TYPE =
            ToObjectFunction.of(ConsumerRecord::timestampType, Type.ofCustom(TimestampType.class));
    private static final ToIntFunction<ConsumerRecord<?, ?>> KEY_SIZE = ConsumerRecord::serializedKeySize;
    private static final ToIntFunction<ConsumerRecord<?, ?>> VALUE_SIZE = ConsumerRecord::serializedValueSize;
    private static final ToObjectFunction<ConsumerRecord<?, ?>, Integer> LEADER_EPOCH =
            ToObjectFunction.of(KafkaRecordColumns::leaderEpoch, Type.intType().boxedType());

    private static Instant timestamp(ConsumerRecord<?, ?> record) {
        return Instant.ofEpochMilli(record.timestamp());
    }

    private static Integer leaderEpoch(ConsumerRecord<?, ?> record) {
        return record.leaderEpoch().orElse(null);
    }

    public static Builder builder() {
        return ImmutableKafkaRecordColumns.builder();
    }

    @Nullable
    @Default
    public String topicColumn() {
        return "KafkaTopic";
    }

    @Nullable
    @Default
    public String partitionColumn() {
        return "KafkaPartition";
    }

    @Nullable
    @Default
    public String offsetColumn() {
        return "KafkaOffset";
    }

    @Nullable
    @Default
    public String timestampColumn() {
        return "KafkaTimestamp";
    }

    @Nullable
    @Default
    public String timestampTypeColumn() {
        return "KafkaTimestampType";
    }

    @Nullable
    @Default
    public String keySizeColumn() {
        return "KafkaKeySize";
    }

    @Nullable
    @Default
    public String valueSizeColumn() {
        return "KafkaValueSize";
    }

    @Nullable
    @Default
    public String leaderEpochColumn() {
        return "KafkaLeaderEpoch";
    }

    public interface Builder {
        Builder topicColumn(String topicColumn);

        Builder partitionColumn(String partitionColumn);

        Builder offsetColumn(String offsetColumn);

        Builder timestampColumn(String timestampColumn);

        Builder timestampTypeColumn(String timestampTypeColumn);

        Builder keySizeColumn(String keySizeColumn);

        Builder valueSizeColumn(String valueSizeColumn);

        Builder leaderEpochColumn(String leaderEpochColumn);

        KafkaRecordColumns build();
    }

    final Map<String, TypedFunction<? super ConsumerRecord<?, ?>>> map() {
        final Map<String, TypedFunction<? super ConsumerRecord<?, ?>>> map = new LinkedHashMap<>();
        if (topicColumn() != null) {
            map.put(topicColumn(), TOPIC);
        }
        if (partitionColumn() != null) {
            map.put(partitionColumn(), PARTITION);
        }
        if (offsetColumn() != null) {
            map.put(offsetColumn(), OFFSET);
        }
        if (timestampColumn() != null) {
            map.put(timestampColumn(), TIMESTAMP);
        }
        if (timestampTypeColumn() != null) {
            map.put(timestampTypeColumn(), TIMESTAMP_TYPE);
        }
        if (keySizeColumn() != null) {
            map.put(keySizeColumn(), KEY_SIZE);
        }
        if (valueSizeColumn() != null) {
            map.put(valueSizeColumn(), VALUE_SIZE);
        }
        if (leaderEpochColumn() != null) {
            map.put(leaderEpochColumn(), LEADER_EPOCH);
        }
        return map;
    }
}
