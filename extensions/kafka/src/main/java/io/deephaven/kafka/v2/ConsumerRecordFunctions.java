/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.util.QueryConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.util.Optional;

class ConsumerRecordFunctions {

    public static String topic(ConsumerRecord<?, ?> record) {
        return record.topic();
    }

    public static int partition(ConsumerRecord<?, ?> record) {
        return record.partition();
    }

    public static long offset(ConsumerRecord<?, ?> record) {
        return record.offset();
    }

    // todo timestamp

    public static byte[] header(ConsumerRecord<?, ?> record, String key) {
        final Header header = record.headers().lastHeader(key);
        return header == null ? null : header.value();
    }

    public static <K> K key(ConsumerRecord<K, ?> record) {
        return record.key();
    }

    public static <V> V value(ConsumerRecord<?, V> record) {
        return record.value();
    }


    public static int leaderEpoch(ConsumerRecord<?, ?> record) {
        final Optional<Integer> leaderEpoch = record.leaderEpoch();
        // noinspection OptionalIsPresent
        if (leaderEpoch.isPresent()) {
            return leaderEpoch.get();
        }
        return QueryConstants.NULL_INT;
    }

    public static int serializedKeySize(ConsumerRecord<?, ?> record) {
        final int size = record.serializedKeySize();
        return size == ConsumerRecord.NULL_SIZE ? QueryConstants.NULL_INT : size;
    }

    public static int serializedValueSize(ConsumerRecord<?, ?> record) {
        final int size = record.serializedValueSize();
        return size == ConsumerRecord.NULL_SIZE ? QueryConstants.NULL_INT : size;
    }

    public static long timestampEpochNanos(ConsumerRecord<?, ?> record) {
        // Technically, very old kafka APIs didn't have timestamps.
        // Kafka 0.10.0.0, released May 22, 2016, was the first release to have timestamps.
        // Unlikely to encounter? The ConsumerRecord#timestamp doesn't even mention.
        final long timestampEpochMillis = record.timestamp();
        // todo: overflow checking?
        return timestampEpochMillis * 1_000_000L;
    }
}
