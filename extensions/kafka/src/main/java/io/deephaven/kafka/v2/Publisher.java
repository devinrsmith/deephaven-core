/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.stream.StreamPublisher;
import org.apache.kafka.common.TopicPartition;

import java.util.Set;

public interface Publisher extends StreamPublisher {

    Set<TopicPartition> topicPartitions();
}
