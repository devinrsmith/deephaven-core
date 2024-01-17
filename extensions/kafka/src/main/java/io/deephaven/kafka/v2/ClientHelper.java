/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class ClientHelper {

    public static Set<TopicPartition> assignAndSeek(KafkaConsumer<?, ?> client, Offsets options) {
        final Map<TopicPartition, Offset> offsets = validatedTopicPartitions(client, (OffsetsBase) options);
        client.assign(offsets.keySet());
        ClientHelper.seek(client, offsets);
        return offsets.keySet();
    }

    static void seek(KafkaConsumer<?, ?> client, Map<TopicPartition, Offset> offsets) {
        final List<TopicPartition> beginning = new ArrayList<>();
        final List<TopicPartition> end = new ArrayList<>();
        final Map<TopicPartition, Long> timestamps = new HashMap<>();
        for (final Entry<TopicPartition, Offset> e : offsets.entrySet()) {
            final Offset offset = e.getValue();
            if (offset == Offset.beginning()) {
                beginning.add(e.getKey());
            } else if (offset == Offset.end()) {
                end.add(e.getKey());
            } else if (offset instanceof OffsetTimestamp) {
                timestamps.put(e.getKey(), ((OffsetTimestamp) offset).kafkaSafeEpochMillis());
            } else if (offset instanceof OffsetReal) {
                client.seek(e.getKey(), ((OffsetReal) offset).offset());
            } else // noinspection StatementWithEmptyBody
            if (offset instanceof OffsetCommitted) {
                // Ignore - if `group.id` is set, poll will auto-seek to the necessary commits, otherwise will fallback
                // to `auto.offset.reset` strategy.
            } else {
                throw new IllegalArgumentException("Invalid Offset type: " + offset.getClass());
            }
        }
        if (!beginning.isEmpty()) {
            // This is more efficient than beginningOffsets + seek
            client.seekToBeginning(beginning);
        }
        if (!end.isEmpty()) {
            // This is more efficient than endOffsets + seek
            client.seekToEnd(end);
        }
        if (!timestamps.isEmpty()) {
            final Map<TopicPartition, OffsetAndTimestamp> map = client.offsetsForTimes(timestamps);
            for (final Entry<TopicPartition, OffsetAndTimestamp> e : map.entrySet()) {
                final OffsetAndTimestamp oat = e.getValue();
                client.seek(e.getKey(), new OffsetAndMetadata(oat.offset(), oat.leaderEpoch(), null));
            }
        }
    }

    static Stream<TopicPartition> topicPartitions(KafkaConsumer<?, ?> client, String topic) {
        // todo: error if empty?
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

    private static Map<TopicPartition, Offset> validatedTopicPartitions(KafkaConsumer<?, ?> client,
            OffsetsBase offsetsOptions) {
        final Set<String> topics = offsetsOptions.topics().collect(Collectors.toSet());
        // Note: it's probably best to take this per-topic approach as opposed to heavier hammer with client.listTopics
        final Map<String, List<PartitionInfo>> partitionInfo = new LinkedHashMap<>(topics.size());
        for (String topic : topics) {
            final List<PartitionInfo> partitions = client.partitionsFor(topic);
            if (partitions.isEmpty()) {
                throw new IllegalArgumentException(String.format("Topic '%s' not found", topic));
            }
            partitionInfo.put(topic, partitions);
        }
        final Map<TopicPartition, Offset> offsets = offsetsOptions.offsets(partitionInfo);
        if (offsets.isEmpty()) {
            throw new IllegalArgumentException("Offsets is empty");
        }
        final Set<TopicPartition> existingTopicPartitions = partitionInfo.values()
                .stream()
                .flatMap(Collection::stream)
                .map(OffsetsBase::topicPartition)
                .collect(Collectors.toSet());
        for (TopicPartition topicPartition : offsets.keySet()) {
            if (!existingTopicPartitions.contains(topicPartition)) {
                throw new IllegalArgumentException(String.format("Topic partition '%s' not found", topicPartition));
            }
        }
        return offsets;
    }
}
