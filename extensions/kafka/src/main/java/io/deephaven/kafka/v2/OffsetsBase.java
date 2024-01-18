/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

abstract class OffsetsBase implements Offsets {

    static TopicPartition topicPartition(PartitionInfo p) {
        return new TopicPartition(p.topic(), p.partition());
    }

    abstract Stream<String> topics();

    abstract Map<TopicPartition, OffsetInternal> offsets(Map<String, List<PartitionInfo>> info);
}
