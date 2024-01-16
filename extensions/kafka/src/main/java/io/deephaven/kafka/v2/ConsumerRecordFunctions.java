/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.util.QueryConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.util.Optional;

public final class ConsumerRecordFunctions {

    /**
     * Returns the last header value for {@code key} if it exists, otherwise returns {@code null}.
     *
     * @param record the record
     * @param key the key
     * @return the value for the last header, may be null
     * @see ConsumerRecord#headers()
     * @see org.apache.kafka.common.header.Headers#lastHeader(String)
     */
    public static byte[] lastHeader(ConsumerRecord<?, ?> record, String key) {
        final Header header = record.headers().lastHeader(key);
        return header == null ? null : header.value();
    }

    /**
     * Returns {@link ConsumerRecord#leaderEpoch()} if it exists, otherwise return {@link QueryConstants#NULL_INT}.
     *
     * @param record the consumer record
     * @return the leader epoch
     */
    public static int leaderEpoch(ConsumerRecord<?, ?> record) {
        final Optional<Integer> leaderEpoch = record.leaderEpoch();
        // noinspection OptionalIsPresent
        if (leaderEpoch.isPresent()) {
            return leaderEpoch.get();
        }
        return QueryConstants.NULL_INT;
    }

    /**
     * Returns {@link ConsumerRecord#serializedKeySize()} when it is not equal to {@value ConsumerRecord#NULL_SIZE},
     * otherwise returns {@link QueryConstants#NULL_INT}.
     *
     * @param record the consumer record
     * @return the serialized key size
     */
    public static int serializedKeySize(ConsumerRecord<?, ?> record) {
        final int size = record.serializedKeySize();
        return size == ConsumerRecord.NULL_SIZE ? QueryConstants.NULL_INT : size;
    }

    /**
     * Returns {@link ConsumerRecord#serializedValueSize()} when it is not equal to {@value ConsumerRecord#NULL_SIZE},
     * otherwise returns {@link QueryConstants#NULL_INT}.
     *
     * @param record the consumer record
     * @return the serialized value size
     */
    public static int serializedValueSize(ConsumerRecord<?, ?> record) {
        final int size = record.serializedValueSize();
        return size == ConsumerRecord.NULL_SIZE ? QueryConstants.NULL_INT : size;
    }

    /**
     * Returns {@link ConsumerRecord#timestamp()} as epoch nanos.
     *
     * @param record the record
     * @return the epoch nanos
     */
    public static long timestampEpochNanos(ConsumerRecord<?, ?> record) {
        final long timestampEpochMillis = record.timestamp();
        return timestampEpochMillis * 1_000_000L;
    }
}
