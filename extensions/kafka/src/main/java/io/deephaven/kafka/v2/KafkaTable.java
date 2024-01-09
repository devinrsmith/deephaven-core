/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

public final class KafkaTable {

    public static TableDefinition definition(KafkaTableOptions<?, ?> options) {
        return options.tableDefinition();
    }

    public static Table of(KafkaTableOptions<?, ?> options) {
        return options.table();
    }

    // todo: generic entry point for non-table based use-cases

    /*
        public static void consume(
            @NotNull final Properties kafkaProperties,
            @NotNull final String topic,
            @NotNull final IntPredicate partitionFilter,
            @NotNull final InitialOffsetLookup partitionToInitialOffset,
            @NotNull final Consume.KeyOrValueSpec keySpec,
            @NotNull final Consume.KeyOrValueSpec valueSpec,
            @NotNull final StreamConsumerRegistrarProvider streamConsumerRegistrarProvider,
            @Nullable final ConsumerLoopCallback consumerLoopCallback) {
     */

    public static Table of(KafkaOptions<?, ?> options) {
        return KafkaOptions.create(options, KafkaTable::createSingleTable);
    }

    public static PartitionedTable ofPartitioned(KafkaOptions<?, ?> options) {
        return KafkaOptions.create(options, KafkaTable::createPartitionedTable);
    }

    private static Table createSingleTable(Map<StreamPublisher, Set<TopicPartition>> map) {
        if (map.size() != 1) {
            throw new IllegalStateException();
        }
        final StreamPublisher publisher = map.keySet().iterator().next();
        // todo table type
        return new StreamToBlinkTableAdapter(null, publisher, null, "name").table();
    }

    private static PartitionedTable createPartitionedTable(Map<StreamPublisher, Set<TopicPartition>> map) {
        throw new RuntimeException("TBD");
    }
}
