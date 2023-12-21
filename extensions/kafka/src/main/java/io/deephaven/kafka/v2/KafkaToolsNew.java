/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.functions.TypedFunction;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.processor.ObjectProcessorFiltered;
import io.deephaven.processor.functions.ObjectProcessorFunctions;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class KafkaToolsNew {

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
            Options<K, V> options,
            ObjectProcessor<ConsumerRecord<K, V>> processor,
            List<String> columnNames) {
        return null;
    }

    public static <K, V> Table of(
            Options<K, V> options,
            ObjectProcessorFiltered<ConsumerRecord<K, V>> processor,
            List<String> columnNames) {
        return null;
    }

    public static <K, V> PartitionedTable ofPartitioned(
            Options<K, V> options,
            ObjectProcessor<ConsumerRecord<K, V>> processor,
            List<String> columnNames) {
        return null;
    }

    public static <K, V> PartitionedTable ofPartitioned(
            Options<K, V> options,
            ObjectProcessorFiltered<ConsumerRecord<K, V>> processor,
            List<String> columnNames) {
        return null;
    }
}
