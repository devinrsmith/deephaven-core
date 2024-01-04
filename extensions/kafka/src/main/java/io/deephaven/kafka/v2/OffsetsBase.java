/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

abstract class OffsetsBase implements Offsets {

    abstract Map<TopicPartition, Offset> offsets(KafkaConsumer<?, ?> client);
}
