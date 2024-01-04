/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.engine.table.Table;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;

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

    public Runnable start(ClientOptions<K, V> options, Collection<TopicPartition> topicPartitions) {
        // todo catch exception
        final KafkaConsumer<K, V> client = options.createClient();
        final Thread thread = new Thread(() -> {
            try {
                client.assign(topicPartitions);
                client.seekToEnd(topicPartitions);
                while (true) {
                    final ConsumerRecords<K, V> records;
                    try {
                        records = client.poll(Duration.ofSeconds(1));
                    } catch (WakeupException e) {
                        notifyFailure(new RuntimeException("closed by Runnable"));
                        return;
                    }
                    if (records.isEmpty()) {
                        continue;
                    }
                    publisher.fill(records);
                }
            } catch (Throwable t) {
                notifyFailure(t);
                throw t;
            }
        });
        thread.setDaemon(true);
        thread.start();
        return client::wakeup;
    }

    private void notifyFailure(Throwable t) {
        try {
            publisher.acceptFailure(t);
        } catch (Throwable t2) {
            t.addSuppressed(t2);
        }
    }
}
