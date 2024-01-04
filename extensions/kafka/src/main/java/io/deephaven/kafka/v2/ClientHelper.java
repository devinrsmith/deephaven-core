/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class ClientHelper {


    // earliest, latest


    public static void assignAndSeek(KafkaConsumer<?, ?> client, SubscribeOptions options) {
        final Map<TopicPartition, Offset> offsets = ClientHelper.offsets(client, options.offsets());
        client.assign(offsets.keySet());
        ClientHelper.seek(client, offsets);
    }

    static void seek(KafkaConsumer<?, ?> client, Map<TopicPartition, Offset> offsets) {
        final List<TopicPartition> beginningTopicPartitions = new ArrayList<>();
        final List<TopicPartition> endTopicPartitions = new ArrayList<>();
        for (Entry<TopicPartition, Offset> e : offsets.entrySet()) {
            final Offset offset = e.getValue();
            if (offset == Offset.beginning()) {
                beginningTopicPartitions.add(e.getKey());
            } else if (offset == Offset.end()) {
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

    static Map<TopicPartition, Offset> offsets(KafkaConsumer<?, ?> client, List<Offsets> subscribes) {
        return subscribes.stream()
                .map(OffsetsBase.class::cast)
                .map(offsetsBase -> offsetsBase.offsets(client))
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }
}
