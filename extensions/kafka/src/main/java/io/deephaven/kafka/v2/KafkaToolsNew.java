/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.engine.context.ExecutionContext;
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
import org.apache.kafka.clients.consumer.KafkaConsumer;
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


    public static <K, V> KafkaPublisher<K, V> of(
            ObjectProcessor<ConsumerRecord<K, V>> processor,
            List<String> columnNames) {
        final TableDefinition tableDefinition = TableDefinition.from(columnNames, processor.outputTypes());
        final Runnable onShutdownCallback = null;
        final KafkaStreamPublisherImpl<K, V> publisher = new KafkaStreamPublisherImpl<>(processor, onShutdownCallback);
        final ExecutionContext executionContext = ExecutionContext.getContext();
        final StreamToBlinkTableAdapter adapter =
                new StreamToBlinkTableAdapter(tableDefinition, publisher, executionContext.getUpdateGraph(), "todo");
        return new KafkaPublisher<>(publisher, adapter);
    }

    public static Table ofTable(TableDefinition tableDefinition, StreamPublisher publisher) {

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

    public static <K, V> Table blinkTable(
            String name,
            UpdateSourceRegistrar updateSourceRegistrar,
            ClientOptions<K, V> clientOptions,
            SubscribeOptions subscribeOptions,
            ObjectProcessor<ConsumerRecord<K, V>> processor,
            List<String> columnNames,
            Map<String, Object> extraAttributes,
            int chunkSize) {
        final KafkaPublisherDriver<K, V> publisher = KafkaPublisherDriver.of(
                clientOptions,
                subscribeOptions,
                new KafkaStreamConsumerAdapter<>(processor, chunkSize));
        final StreamToBlinkTableAdapter adapter;
        try {
            // noinspection resource
            adapter = new StreamToBlinkTableAdapter(
                    TableDefinition.from(columnNames, processor.outputTypes()),
                    publisher,
                    updateSourceRegistrar,
                    name,
                    extraAttributes);
            publisher.start();
        } catch (Throwable t) {
            publisher.startError(t);
            throw t;
        }
        return adapter.table();
    }

    public static Table blinkTable(TableOptions<?, ?> options) {
        return options.subscribe();
    }

    static void safeCloseClient(Throwable t, KafkaConsumer<?, ?> client) {
        try {
            client.close();
        } catch (Throwable t2) {
            t.addSuppressed(t2);
        }
    }
}
