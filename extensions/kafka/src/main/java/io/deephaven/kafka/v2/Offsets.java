/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public interface Offsets {

    /**
     * The earliest offsets for all partitions of {@code topic}.
     *
     * @param topic the topic
     * @return the offsets
     */
    static Offsets earliest(String topic) {
        return ImmutableOffsetsEarliest.of(topic);
    }

    /**
     * The latest offsets for all partitions of {@code topic}.
     *
     * @param topic the topic
     * @return the offsets
     */
    static Offsets latest(String topic) {
        return ImmutableOffsetsLatest.of(topic);
    }

    /**
     * The specific offsets for each partition in {@code offsets}.
     *
     * @param offsets the offsets
     * @return the offsets
     */
    static Offsets of(Map<TopicPartition, Offset> offsets) {
        return ImmutableOffsetsExplicitImpl.of(offsets);
    }

    /**
     * The combined {@code offsets}.
     *
     * @param offsets the offsets
     * @return the offsets
     */
    static Offsets of(Offsets... offsets) {
        return of(Arrays.asList(offsets));
    }

    /**
     * The combined {@code offsets}.
     *
     * @param offsets the offsets
     * @return the offsets
     */
    static Offsets of(List<Offsets> offsets) {
        return ImmutableOffsetsList.of(offsets);
    }
}
