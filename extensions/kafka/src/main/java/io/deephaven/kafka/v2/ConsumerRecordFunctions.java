/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.util.QueryConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

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

    public static int serializedKeySize(ConsumerRecord<?, ?> record) {
        final int keySize = record.serializedKeySize();
        return keySize == -1 ? QueryConstants.NULL_INT : keySize;
    }

    public static int serializedValueSize(ConsumerRecord<?, ?> record) {
        final int recordSize = record.serializedValueSize();
        return recordSize == -1 ? QueryConstants.NULL_INT : recordSize;
    }

    public static int leaderEpoch(ConsumerRecord<?, ?> record) {
        return record.leaderEpoch().orElse(QueryConstants.NULL_INT);
    }
}
