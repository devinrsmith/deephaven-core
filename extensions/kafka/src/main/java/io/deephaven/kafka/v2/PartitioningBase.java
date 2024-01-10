/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.kafka.v2.KafkaOptions.Partitioning;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

abstract class PartitioningBase implements Partitioning {

    public abstract Stream<Set<TopicPartition>> partition(Set<TopicPartition> topicPartitions);
}
