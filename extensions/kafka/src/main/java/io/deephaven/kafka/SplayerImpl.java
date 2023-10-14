package io.deephaven.kafka;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ObjectChunksOneToOne;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.kafka.KafkaTools.Consume.KeyOrValueSpec;
import io.deephaven.kafka.KafkaTools.KeyOrValue;
import io.deephaven.kafka.KafkaTools.KeyOrValueIngestData;
import io.deephaven.kafka.ingest.KeyOrValueProcessor;
import io.deephaven.kafka.ingest.MultiFieldChunkAdapter;
import io.deephaven.qst.type.Type;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

class SplayerImpl<T> extends KeyOrValueSpec implements KeyOrValueProcessor {

    private final Deserializer<T> deserializer;
    private final ObjectChunksOneToOne<T> splayer;
    private final List<String> columnNames;
    private Function<WritableChunk<?>[], List<WritableChunk<?>>> offsetsAdapter;

    SplayerImpl(Deserializer<T> deserializer, ObjectChunksOneToOne<T> splayer, List<String> columnNames) {
        if (columnNames.size() != splayer.outputTypes().size()) {
            throw new IllegalArgumentException();
        }
        if (columnNames.stream().distinct().count() != columnNames.size()) {
            throw new IllegalArgumentException();
        }
        this.deserializer = Objects.requireNonNull(deserializer);
        this.splayer = Objects.requireNonNull(splayer);
        this.columnNames = List.copyOf(columnNames);
    }

    @Override
    public Optional<SchemaProvider> getSchemaProvider() {
        return Optional.empty();
    }

    @Override
    protected Deserializer<T> getDeserializer(KeyOrValue keyOrValue, SchemaRegistryClient schemaRegistryClient,
            Map<String, ?> configs) {
        return deserializer;
    }

    @Override
    protected KeyOrValueIngestData getIngestData(KeyOrValue keyOrValue, SchemaRegistryClient schemaRegistryClient,
            Map<String, ?> configs, MutableInt nextColumnIndexMut, List<ColumnDefinition<?>> columnDefinitionsOut) {
        final KeyOrValueIngestData data = new KeyOrValueIngestData();
        data.fieldPathToColumnName = new LinkedHashMap<>();
        final int L = columnNames.size();
        for (int i = 0; i < L; ++i) {
            final String columnName = columnNames.get(i);
            final Type<?> type = splayer.outputTypes().get(i);
            data.fieldPathToColumnName.put(columnName, columnName);
            columnDefinitionsOut.add(ColumnDefinition.of(columnName, type));
        }
        return data;
    }

    @Override
    protected KeyOrValueProcessor getProcessor(TableDefinition tableDef, KeyOrValueIngestData data) {
        offsetsAdapter = offsetsFunction(MultiFieldChunkAdapter.chunkOffsets(tableDef, data.fieldPathToColumnName));
        return this;
    }

    @Override
    public void handleChunk(ObjectChunk<Object, Values> inputChunk, WritableChunk<Values>[] publisherChunks) {
        // noinspection unchecked
        final ObjectChunk<T, ?> in = (ObjectChunk<T, ?>) inputChunk;
        // eventually, implementation of KafkaStreamPublisher should be re-written so we don't have KeyOrValueSpec
        splayer.splayAll(in, offsetsAdapter.apply(publisherChunks));
    }

    private static <T> Function<T[], List<T>> offsetsFunction(int[] offsets) {
        return offsets.length == 0
                ? array -> Collections.emptyList()
                : isInOrder(offsets)
                        ? array -> Arrays.asList(array).subList(offsets[0], offsets[0] + offsets.length)
                        : array -> reorder(array, offsets);
    }

    private static boolean isInOrder(int[] offsets) {
        for (int i = 1; i < offsets.length; ++i) {
            if (offsets[i - 1] + 1 != offsets[i]) {
                return false;
            }
        }
        return true;
    }

    private static <T> List<T> reorder(T[] array, int[] offsets) {
        final List<T> out = new ArrayList<>(offsets.length);
        for (int offset : offsets) {
            out.add(array[offset]);
        }
        return out;
    }
}
