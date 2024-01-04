/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.annotations.SimpleStyle;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.Map;

@Immutable
@SimpleStyle
abstract class OffsetsExplicitImpl extends OffsetsBase {

    @Parameter
    public abstract Map<TopicPartition, Offset> topicPartitionOffsets();

    @Override
    final Map<TopicPartition, Offset> offsets(KafkaConsumer<?, ?> client) {
        return topicPartitionOffsets();
    }
}
