/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class TableOptions<K, V> {

    public static <K, V> Builder<K, V> builder() {
        return ImmutableTableOptions.builder();
    }

    @Default
    public String name() {
        return "todo";
    }

    public abstract ClientOptions<K, V> clientOptions();

    public abstract SubscribeOptions subscribeOptions();

    // todo: give easy way for users to construct w/ specs for specific key / value types
    public abstract ObjectProcessor<ConsumerRecord<K, V>> processor();

    public abstract List<String> columnNames();

    public abstract Map<String, Object> extraAttributes();

    @Default
    public UpdateSourceRegistrar updateSourceRegistrar() {
        return ExecutionContext.getContext().getUpdateGraph();
    }

    @Default
    public int chunkSize() {
        return ArrayBackedColumnSource.BLOCK_SIZE;
    }

    @Default
    public boolean receiveTimestamp() {
        return true;
    }

    public interface Builder<K, V> {
        Builder<K, V> name(String name);

        Builder<K, V> clientOptions(ClientOptions<K, V> clientOptions);

        Builder<K, V> subscribeOptions(SubscribeOptions subscribeOptions);

        Builder<K, V> processor(ObjectProcessor<ConsumerRecord<K, V>> processor);

        Builder<K, V> addColumnNames(String element);

        Builder<K, V> addColumnNames(String... elements);

        Builder<K, V> addAllColumnNames(Iterable<String> elements);

        Builder<K, V> putExtraAttributes(String key, Object value);

        Builder<K, V> putExtraAttributes(Map.Entry<String, ?> entry);

        Builder<K, V> putAllExtraAttributes(Map<String, ?> entries);

        Builder<K, V> updateSourceRegistrar(UpdateSourceRegistrar updateSourceRegistrar);

        Builder<K, V> chunkSize(int chunkSize);

        TableOptions<K, V> build();
    }

    @Check
    final void checkSize() {
        if (processor().size() != columnNames().size()) {
            throw new IllegalArgumentException();
        }
    }

    @Check
    final void checkChunkSize() {
        if (chunkSize() < 1) {
            throw new IllegalArgumentException("chunkSize must be positive");
        }
    }

    final StreamToBlinkTableAdapter adapter() {
        final TableDefinition definition = receiveTimestamp()
                ? TableDefinition.from(Stream.concat(
                        Stream.of("ReceiveTimestamp"), columnNames().stream()).collect(Collectors.toList()),
                        Stream.concat(Stream.of(Type.instantType()), processor().outputTypes().stream()).collect(Collectors.toList()))
                : TableDefinition.from(columnNames(), processor().outputTypes());
        final KafkaPublisherDriver<K, V> publisher = KafkaPublisherDriver.of(
                clientOptions(),
                subscribeOptions(),
                new KafkaStreamConsumerAdapter<>(processor(), chunkSize(), receiveTimestamp()));
        try {
            final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(
                    definition,
                    publisher,
                    updateSourceRegistrar(),
                    name(),
                    extraAttributes());
            publisher.start();
            return adapter;
        } catch (Throwable t) {
            publisher.startError(t);
            throw t;
        }
    }

    final Table table() {
        // noinspection resource
        return adapter().table();
    }
}
