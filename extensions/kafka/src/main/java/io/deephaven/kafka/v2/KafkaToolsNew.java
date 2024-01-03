/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.functions.TypedFunction;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.processor.ObjectProcessorFiltered;
import io.deephaven.processor.functions.ObjectProcessorFunctions;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

    public static <V> ObjectProcessor<ConsumerRecord<?, V>> what(ObjectProcessor<V> valueProcessor) {
        return ObjectProcessor.combined(List.of(
                ConsumerRecordOptions.classic().processor(),
                Processors.value(valueProcessor)));
    }


    public static <K, V> Table of(
            Options<K, V> options,
            ObjectProcessor<ConsumerRecord<K, V>> processor,
            List<String> columnNames) {


        final TableDefinition tableDefinition = TableDefinition.from(columnNames, processor.outputTypes());
        final StreamPublisher streamPublisher = null;
        final UpdateSourceRegistrar updateSourceRegistrar = null;
        final String name = null;
        final StreamToBlinkTableAdapter adapter =
                new StreamToBlinkTableAdapter(tableDefinition, streamPublisher, updateSourceRegistrar, name);


        return null;
    }

    public static <K, V> Table of(
            Options<K, V> options,
            ObjectProcessorFiltered<ConsumerRecord<K, V>> processor,
            List<String> columnNames) {
        return null;
    }

    public static TableDefinition of(Map<String, Type<?>> map) {

        final Iterator<? extends ColumnHeader<?>> it =
                map.entrySet().stream().map(e -> ColumnHeader.of(e.getKey(), e.getValue())).iterator();

        final Iterable<? extends ColumnHeader<?>> headers = new Iterable<ColumnHeader<?>>() {
            @NotNull
            @Override
            public Iterator<ColumnHeader<?>> iterator() {
                final Iterator<? extends ColumnHeader<?>> actual =
                        map.entrySet().stream().map(e -> ColumnHeader.of(e.getKey(), e.getValue())).iterator();
                // return actual;
                return null;
            }
        };
        return TableDefinition.from(headers);
    }
}
