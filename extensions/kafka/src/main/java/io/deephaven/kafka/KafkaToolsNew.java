/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka;

import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.functions.ToObjectFunction;
import io.deephaven.functions.TypedFunction;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.processor.ObjectProcessorFiltered;
import io.deephaven.processor.functions.ObjectProcessorFunctions;
import io.deephaven.qst.type.GenericType;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public class KafkaToolsNew {

    public abstract class KafkaOptions {
        /*
         * @NotNull final Properties kafkaProperties,
         * 
         * @NotNull final String topic,
         * 
         * @NotNull final IntPredicate partitionFilter,
         * 
         * @NotNull final IntToLongFunction partitionToInitialOffset,
         * 
         * @NotNull final Consume.KeyOrValueSpec keySpec,
         * 
         * @NotNull final Consume.KeyOrValueSpec valueSpec,
         * 
         * @NotNull final TableType tableType
         */
    }

    public static <K, V> ObjectProcessor<ConsumerRecord<K, V>> of(
            Collection<TypedFunction<? super ConsumerRecord<?, ?>>> recordFunctions,
            Collection<TypedFunction<? super K>> keyFunctions,
            Collection<TypedFunction<? super V>> valueFunctions) {
        final List<TypedFunction<? super ConsumerRecord<K, V>>> functions = new ArrayList<>(recordFunctions);
        for (TypedFunction<? super K> keyFunction : keyFunctions) {
            functions.add(TypedFunction.map(ConsumerRecord::key, keyFunction));
        }
        for (TypedFunction<? super V> valueFunction : valueFunctions) {
            functions.add(TypedFunction.map(ConsumerRecord::value, valueFunction));
        }
        return ObjectProcessorFunctions.of(functions);
    }


    public static <K, V> Table of(
            KafkaOptions options,
            ObjectProcessor<ConsumerRecord<K, V>> processor,
            List<String> columnNames) {
        return null;
    }

    public static <K, V> Table of(
            KafkaOptions options,
            ObjectProcessorFiltered<ConsumerRecord<K, V>> processor,
            List<String> columnNames) {
        return null;
    }

    public static <K, V> PartitionedTable ofPartitioned(
            KafkaOptions options,
            ObjectProcessor<ConsumerRecord<K, V>> processor,
            List<String> columnNames) {
        return null;
    }

    public static <K, V> PartitionedTable ofPartitioned(
            KafkaOptions options,
            ObjectProcessorFiltered<ConsumerRecord<K, V>> processor,
            List<String> columnNames) {
        return null;
    }
}
