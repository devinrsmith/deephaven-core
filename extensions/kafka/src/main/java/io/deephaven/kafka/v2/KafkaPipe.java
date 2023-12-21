/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;

class KafkaPipe<K, V> {

    private final KafkaConsumer<K, V> consumer = null;
    private final Accum<ConsumerRecord<K, V>> accum = null;

    public void drive() {
        final ConsumerRecords<K, V> records;
        try {
            records = consumer.poll(Duration.ofSeconds(1));
        } catch (WakeupException e) {
            return;
        }
        if (records.isEmpty()) {
            return;
        }
        // todo: assume all going to this for now
        // can add more differentiation in later
        accum.accumulate(records);
    }
}
