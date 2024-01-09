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
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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

    private boolean test(Entry<TopicPartition, Offset> e) {
        return filter().test(e.getKey());
    }
}
