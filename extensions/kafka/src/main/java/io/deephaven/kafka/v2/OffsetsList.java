/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.annotations.SimpleStyle;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Immutable
@SimpleStyle
abstract class OffsetsList extends OffsetsBase {

    @Parameter
    public abstract List<Offsets> offsets();

    @Override
    final Map<TopicPartition, Offset> offsets(KafkaConsumer<?, ?> client) {
        return offsets().stream()
                .map(OffsetsBase.class::cast)
                .map(offsetsBase -> offsetsBase.offsets(client))
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    @Override
    final Stream<String> topics() {
        return offsets().stream().map(OffsetsBase.class::cast).flatMap(OffsetsBase::topics);
    }

    @Override
    final Map<TopicPartition, Offset> offsets(Map<String, List<PartitionInfo>> info) {
        return offsets().stream()
                .map(OffsetsBase.class::cast)
                .map(offsetsBase -> offsetsBase.offsets(info))
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }
}
