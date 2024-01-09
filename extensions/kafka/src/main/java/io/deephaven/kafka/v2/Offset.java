/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.base.clock.Clock;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public interface Offset {
    /**
     * The beginning offset.
     *
     * @return the beginning offset
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#beginningOffsets(Collection)
     */
    static Offset beginning() {
        return OffsetImpl.BEGINNING;
    }

    /**
     * The end offsets.
     *
     * @return the end offset
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#endOffsets(Collection)
     */
    static Offset end() {
        return OffsetImpl.END;
    }

    /**
     * The last committed offsets (whether the commit happened by this process or another).
     *
     * @return the committed offset
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#committed(Set)
     */
    static Offset committed() {
        return ImmutableOffsetCommitted.of();
    }

    /**
     * The earliest offset whose timestamp is greater than or equal to {@code since}.
     *
     * @param since the timestamp
     * @return the timestamp offset
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#offsetsForTimes(Map)
     */
    static Offset timestamp(Instant since) {
        return ImmutableOffsetTimestamp.of(since);
    }


    /**
     * The earliest offset whose timestamp is at most {@code age} old. Equivalent to
     * {@code timestamp(Clock.system().instantMillis().minus(ago))}.
     *
     * @param ago the age
     * @return the timestamp offset
     * @see #timestamp(Instant)
     */
    static Offset timestamp(Duration ago) {
        return timestamp(Clock.system().instantMillis().minus(ago));
    }

    /**
     * The explicit offset.
     *
     * @param offset the offset
     * @return the offset
     */
    static Offset of(long offset) {
        return ImmutableOffsetReal.of(offset);
    }
}
