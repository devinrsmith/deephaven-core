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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

final class ClientHelper {

    public static final Comparator<TopicPartition> TOPIC_PARTITION_COMPARATOR =
            Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition);

    public static Set<TopicPartition> assignAndSeek(KafkaConsumer<?, ?> client, Offsets options) {
        final TreeMap<TopicPartition, OffsetInternal> offsets = validatedTopicPartitions(client, (OffsetsBase) options);
        // TreeMap to ensure consistent order wrt client API calls
        client.assign(offsets.keySet());
        ClientHelper.seek(client, offsets);
        return offsets.keySet();
    }

    static void seek(KafkaConsumer<?, ?> client, TreeMap<TopicPartition, OffsetInternal> offsets) {
        final List<TopicPartition> beginning = new ArrayList<>();
        final List<TopicPartition> end = new ArrayList<>();
        final Map<TopicPartition, Long> timestamps = new HashMap<>();
        for (final Entry<TopicPartition, OffsetInternal> e : offsets.entrySet()) {
            final OffsetInternal offset = e.getValue();
            if (offset == OffsetInternal.beginning()) {
                beginning.add(e.getKey());
            } else if (offset == OffsetInternal.end()) {
                end.add(e.getKey());
            } else // noinspection StatementWithEmptyBody
            if (offset == OffsetInternal.committed()) {
                // Ignore - if `group.id` is set, poll will auto-seek to the necessary commits, otherwise will fallback
                // to `auto.offset.reset` strategy.
            } else if (offset instanceof OffsetTimestamp) {
                timestamps.put(e.getKey(), ((OffsetTimestamp) offset).kafkaSafeEpochMillis());
            } else if (offset instanceof OffsetExplicit) {
                client.seek(e.getKey(), ((OffsetExplicit) offset).offset());
            } else {
                throw new IllegalStateException("Invalid Offset type: " + offset.getClass());
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

    static void safeCloseClient(KafkaConsumer<?, ?> client, Throwable t) {
        try {
            client.close();
        } catch (Throwable t2) {
            t.addSuppressed(t2);
        }
    }

    private static TreeMap<TopicPartition, OffsetInternal> validatedTopicPartitions(KafkaConsumer<?, ?> client,
            OffsetsBase offsetsOptions) {
        // SortedSet to ensure consistent order wrt client API calls
        final SortedSet<String> topics = offsetsOptions.topics().collect(Collectors.toCollection(TreeSet::new));
        // Note: this per-topic client.partitionsFor() approach is taken as it's probably better than heavy-hammer
        // client.listTopics()
        final Map<String, List<PartitionInfo>> partitionInfo = new HashMap<>(topics.size());
        for (String topic : topics) {
            final List<PartitionInfo> partitions = client.partitionsFor(topic);
            if (partitions.isEmpty()) {
                throw new IllegalArgumentException(String.format("Topic '%s' not found", topic));
            }
            partitionInfo.put(topic, partitions);
        }
        final Map<TopicPartition, OffsetInternal> offsets = offsetsOptions.offsets(partitionInfo);
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
        final TreeMap<TopicPartition, OffsetInternal> sortedMap = new TreeMap<>(TOPIC_PARTITION_COMPARATOR);
        sortedMap.putAll(offsets);
        return sortedMap;
    }
}
