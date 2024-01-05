/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

final class ClientHelper {

    public static void assignAndSeek(KafkaConsumer<?, ?> client, Offsets options) {
        final Map<TopicPartition, Offset> offsets = ((OffsetsBase) options).offsets(client);
        client.assign(offsets.keySet());
        ClientHelper.seek(client, offsets);
    }

    static void seek(KafkaConsumer<?, ?> client, Map<TopicPartition, Offset> offsets) {
        final List<TopicPartition> beginningTopicPartitions = new ArrayList<>();
        final List<TopicPartition> endTopicPartitions = new ArrayList<>();
        for (Entry<TopicPartition, Offset> e : offsets.entrySet()) {
            final Offset offset = e.getValue();
            if (offset == Offset.earliest()) {
                beginningTopicPartitions.add(e.getKey());
            } else if (offset == Offset.latest()) {
                endTopicPartitions.add(e.getKey());
            } else {
                client.seek(e.getKey(), ((OffsetReal) offset).offset());
            }
        }
        if (!beginningTopicPartitions.isEmpty()) {
            client.seekToBeginning(beginningTopicPartitions);
        }
        if (!endTopicPartitions.isEmpty()) {
            client.seekToEnd(endTopicPartitions);
        }
    }

    static Stream<TopicPartition> topicPartitions(KafkaConsumer<?, ?> client, String topic) {
        return client
                .partitionsFor(topic)
                .stream()
                .map(ClientHelper::topicPartition);
    }

    static TopicPartition topicPartition(PartitionInfo p) {
        return new TopicPartition(p.topic(), p.partition());
    }

    static void safeCloseClient(KafkaConsumer<?, ?> client, Throwable t) {
        try {
            client.close();
        } catch (Throwable t2) {
            t.addSuppressed(t2);
        }
    }
}
