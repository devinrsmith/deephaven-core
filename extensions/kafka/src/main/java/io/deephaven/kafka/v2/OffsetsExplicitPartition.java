/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.annotations.SimpleStyle;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Immutable
@SimpleStyle
abstract class OffsetsExplicitPartition extends OffsetsBase {

    @Parameter
    public abstract TopicPartition topicPartition();

    @Parameter
    public abstract long offset();

    @Override
    final Stream<String> topics() {
        return Stream.of(topicPartition().topic());
    }

    @Override
    final Map<TopicPartition, OffsetInternal> offsets(Map<String, List<PartitionInfo>> info) {
        return Map.of(topicPartition(), OffsetInternal.of(offset()));
    }
}
