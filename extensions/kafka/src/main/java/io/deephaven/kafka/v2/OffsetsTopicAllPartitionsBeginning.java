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
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.deephaven.kafka.v2.ClientHelper.topicPartitions;

@Immutable
@SimpleStyle
abstract class OffsetsTopicAllPartitionsBeginning extends OffsetsBase {

    @Parameter
    public abstract String topic();

    @Override
    final Map<TopicPartition, Offset> offsets(KafkaConsumer<?, ?> client) {
        return topicPartitions(client, topic())
                .collect(Collectors.toMap(Function.identity(), tp -> Offset.beginning()));
    }
}
