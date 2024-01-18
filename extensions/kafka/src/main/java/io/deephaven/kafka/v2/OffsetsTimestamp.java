/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.annotations.SimpleStyle;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Immutable
@SimpleStyle
abstract class OffsetsTimestamp extends OffsetsBase {

    @Parameter
    public abstract String topic();

    @Parameter
    public abstract Instant since();

    @Override
    final Stream<String> topics() {
        return Stream.of(topic());
    }

    @Override
    final Map<TopicPartition, OffsetInternal> offsets(Map<String, List<PartitionInfo>> info) {
        return info.get(topic())
                .stream()
                .map(OffsetsBase::topicPartition)
                .collect(Collectors.toMap(Function.identity(), tp -> OffsetInternal.timestamp(since())));
    }
}
