/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.annotations.SimpleStyle;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

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
}
