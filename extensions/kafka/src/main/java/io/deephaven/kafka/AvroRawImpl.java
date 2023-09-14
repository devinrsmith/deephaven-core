/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.ColumnName;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.functions.ToObjectFunction;
import io.deephaven.functions.TypedFunction;
import io.deephaven.kafka.AvroSchemaDecoder.FieldFunction;
import io.deephaven.kafka.KafkaTools.Consume;
import io.deephaven.kafka.KafkaTools.KeyOrValue;
import io.deephaven.kafka.KafkaTools.KeyOrValueIngestData;
import io.deephaven.kafka.ingest.FieldCopier;
import io.deephaven.kafka.ingest.FieldCopierAdapter;
import io.deephaven.kafka.ingest.KeyOrValueProcessor;
import io.deephaven.kafka.ingest.MultiFieldChunkAdapter;
import io.deephaven.kafka.protobuf.ProtobufConsumeOptions;
import io.deephaven.qst.type.Type;
import org.apache.avro.Schema;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

class AvroRawImpl {

    static final class ConsumeImpl extends Consume.KeyOrValueSpec {

        private static final ToObjectFunction<Object, AvroParsingState> AVRO_PARSING_STATE_OBJ =
                ToObjectFunction.identity(Type.ofCustom(AvroParsingState.class));

        private final ProtobufConsumeOptions specs;

        ConsumeImpl(ProtobufConsumeOptions specs) {
            if (specs.schemaMessageName().isPresent()) {
                throw new IllegalArgumentException("not applicable for avro - todo make type");
            }
            this.specs = Objects.requireNonNull(specs);
        }

        @Override
        public Optional<SchemaProvider> getSchemaProvider() {
            return Optional.of(new AvroSchemaProvider());
        }

        @Override
        Deserializer<?> getDeserializer(KeyOrValue keyOrValue, SchemaRegistryClient schemaRegistryClient,
                Map<String, ?> configs) {
            // Need to skip the first 5 bytes, not handling dynamic schemas ATM.
            // https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
            return (Deserializer<AvroParsingState>) (topic, data) -> new AvroParsingState(data, new MutableInt(5));
        }

        @Override
        KeyOrValueIngestData getIngestData(KeyOrValue keyOrValue, SchemaRegistryClient schemaRegistryClient,
                Map<String, ?> configs, MutableInt nextColumnIndexMut, List<ColumnDefinition<?>> columnDefinitionsOut) {
            final Schema schema;
            try {
                schema = getSchema(schemaRegistryClient);
            } catch (RestClientException | IOException e) {
                throw new UncheckedDeephavenException(e);
            }
            // note: this implementation is predicated on the engine calling these functions *in* order.
            // TODO: we need to guarantee that to support incremental parsing states like this example
            final List<FieldFunction> functions = AvroSchemaDecoder.of(schema);
            final List<FieldCopier> fieldCopiers = new ArrayList<>(functions.size());
            final KeyOrValueIngestData data = new KeyOrValueIngestData();
            data.fieldPathToColumnName = new LinkedHashMap<>();
            for (FieldFunction f : functions) {
                add(ColumnName.of(f.field().name()), f.function(), data, columnDefinitionsOut, fieldCopiers);
            }
            // we don't have enough info at this time to create KeyOrValueProcessorImpl
            // data.extra = new KeyOrValueProcessorImpl(MultiFieldChunkAdapter.chunkOffsets(null, null), fieldCopiers,
            // false);
            data.extra = fieldCopiers;
            return data;
        }

        private void add(
                ColumnName columnName,
                TypedFunction<AvroParsingState> function,
                KeyOrValueIngestData data,
                List<ColumnDefinition<?>> columnDefinitionsOut,
                List<FieldCopier> fieldCopiersOut) {
            data.fieldPathToColumnName.put(columnName.name(), columnName.name());
            columnDefinitionsOut.add(ColumnDefinition.of(columnName.name(), function.returnType()));
            fieldCopiersOut.add(FieldCopierAdapter.of(AVRO_PARSING_STATE_OBJ.map(ToChunkTypeTransform.of(function))));
        }

        @Override
        KeyOrValueProcessor getProcessor(TableDefinition tableDef, KeyOrValueIngestData data) {
            // noinspection unchecked
            return new KeyOrValueProcessorImpl(
                    MultiFieldChunkAdapter.chunkOffsets(tableDef, data.fieldPathToColumnName),
                    (List<FieldCopier>) data.extra, false);
        }

        private Schema getSchema(SchemaRegistryClient schemaRegistryClient)
                throws RestClientException, IOException {
            final SchemaMetadata metadata = specs.schemaVersion().isPresent()
                    ? schemaRegistryClient.getSchemaMetadata(specs.schemaSubject(), specs.schemaVersion().getAsInt())
                    : schemaRegistryClient.getLatestSchemaMetadata(specs.schemaSubject());
            if (!AvroSchema.TYPE.equals(metadata.getSchemaType())) {
                throw new IllegalStateException(String.format("Expected schema type %s but was %s", AvroSchema.TYPE,
                        metadata.getSchemaType()));
            }
            final AvroSchema avroSchema = (AvroSchema) schemaRegistryClient
                    .getSchemaBySubjectAndId(specs.schemaSubject(), metadata.getId());
            return avroSchema.rawSchema();
        }
    }
}
