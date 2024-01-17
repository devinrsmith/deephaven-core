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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Immutable
@SimpleStyle
abstract class OffsetsFiltered extends OffsetsBase {

    @Parameter
    public abstract Offsets offsets();

    @Parameter
    public abstract Predicate<TopicPartition> filter();

    @Override
    final Map<TopicPartition, Offset> offsets(KafkaConsumer<?, ?> client) {
        return ((OffsetsBase) offsets())
                .offsets(client)
                .entrySet()
                .stream()
                .filter(this::test)
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    @Override
    final Stream<String> topics() {
        // Can't apply the filter b/c we can't materialize TopicPartitions
        return ((OffsetsBase) offsets()).topics();
    }

    @Override
    final Map<TopicPartition, Offset> offsets(Map<String, List<PartitionInfo>> info) {
        return ((OffsetsBase) offsets())
                .offsets(info)
                .entrySet().stream()
                .filter(this::test)
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    private boolean test(Entry<TopicPartition, Offset> e) {
        return filter().test(e.getKey());
    }
}
