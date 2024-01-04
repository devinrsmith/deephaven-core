/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.engine.table.Table;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;

public final class KafkaPublisher<K, V> {


    private final KafkaStreamPublisherImpl<K, V> publisher;
    private final StreamToBlinkTableAdapter adapter;

    KafkaPublisher(KafkaStreamPublisherImpl<K, V> publisher, StreamToBlinkTableAdapter adapter) {
        this.publisher = publisher;
        this.adapter = adapter;
    }

    KafkaStreamPublisherImpl<K, V> publisher() {
        return publisher;
    }

    public Table table() {
        return adapter.table();
    }

    public void start(ClientOptions<K, V> options, Collection<TopicPartition> topicPartitions) {
        // todo
        final KafkaConsumer<K, V> client = options.createClient();
        client.assign(topicPartitions);
        client.seekToEnd(topicPartitions);
        final Thread thread = new Thread(() -> {
            while (true) {
                final ConsumerRecords<K, V> records = client.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    continue;
                }
                publisher.fill(records);
            }
        });
        thread.setDaemon(true);
        thread.start();
    }
}
