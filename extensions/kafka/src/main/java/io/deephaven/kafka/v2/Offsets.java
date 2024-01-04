/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public interface Offsets {

    static Offsets beginning(String topic) {
        return ImmutableOffsetsTopicAllPartitionsBeginning.of(topic);
    }

    static Offsets end(String topic) {
        return ImmutableOffsetsTopicAllPartitionsEnd.of(topic);
    }

    static Offsets of(Map<TopicPartition, Offset> offsets) {
        return ImmutableOffsetsExplicitImpl.of(offsets);
    }
}
