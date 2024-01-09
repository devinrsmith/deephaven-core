/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.base.clock.Clock;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Offsets {

    /**
     * The beginning offsets for all partitions of {@code topic}.
     *
     * @param topic the topic
     * @return the beginning offsets
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#beginningOffsets(Collection)
     */
    static Offsets beginning(String topic) {
        return ImmutableOffsetsBeginning.of(topic);
    }

    /**
     * The end offsets for all partitions of {@code topic}.
     *
     * @param topic the topic
     * @return the end offsets
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#endOffsets(Collection)
     */
    static Offsets end(String topic) {
        return ImmutableOffsetsEnd.of(topic);
    }

    /**
     * The committed offset for all partitions of {@code topic} (whether the commits happened by this process or
     * another).
     *
     * @param topic the topic
     * @return the committed offsets
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#committed(Set)
     */
    static Offsets committed(String topic) {
        return ImmutableOffsetsCommitted.of(topic);
    }

    /**
     * The earliest offset whose timestamp is greater than or equal to {@code since} for all partitions of
     * {@code topic}.
     * 
     * @param topic the topic
     * @param since the timestamp
     * @return the timestamp offsets
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#offsetsForTimes(Map)
     */
    static Offsets timestamp(String topic, Instant since) {
        return ImmutableOffsetsTimestamp.of(topic, since);
    }

    /**
     * The earliest offset whose timestamp is at most {@code age} old. Equivalent to
     * {@code timestamp(Clock.system().instantMillis().minus(ago))}.
     *
     * @param ago the age
     * @return the timestamp offset
     * @see #timestamp(String, Instant)
     */
    static Offsets timestamp(String topic, Duration ago) {
        return timestamp(topic, Clock.system().instantMillis().minus(ago));
    }

    /**
     * The specific offsets for each topic partition in {@code offsets}.
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
