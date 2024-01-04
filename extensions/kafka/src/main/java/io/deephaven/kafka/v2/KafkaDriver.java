/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.processor.ObjectProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Objects;

final class KafkaDriver<K, V> {

    static <K, V> void what(
            ObjectProcessor<ConsumerRecord<K, V>> processor,
            ClientOptions<K, V> clientOptions) {
        final KafkaConsumer<K, V> client = clientOptions.createClient();
        final KafkaStreamPublisherImpl<K, V> publisher = new KafkaStreamPublisherImpl<>(processor);
        new KafkaDriver<>(client, publisher);

    }

    private final KafkaConsumer<K, V> client;
    private final KafkaStreamPublisherImpl<K, V> publisher;

    public KafkaDriver(KafkaConsumer<K, V> client, KafkaStreamPublisherImpl<K, V> publisher) {
        this.client = Objects.requireNonNull(client);
        this.publisher = Objects.requireNonNull(publisher);
    }

    public void runOnce(Duration timeout) {
        final ConsumerRecords<K, V> records;
        try {
            records = client.poll(timeout);
        } catch (WakeupException | InterruptException e) {
            return;
        }
        if (records.isEmpty()) {
            return;
        }
        publisher.fill(records);
    }



}
